// SPDX-License-Identifier: GPL-2.0
/*
 * RCU mixed-workload generator derived from:
 *   https://github.com/sbcd90/kernel-module-rcu-test
 *
 * The module intentionally does not define private trace events.  Each
 * successful update retires one struct book through call_rcu(), so kernels
 * built with the RCU trace events expose the normal events:
 *
 *   rcu:rcu_callback
 *   rcu:rcu_invoke_callback
 *
 * Both events contain the same rcu_head pointer (rhp) and callback-function
 * pointer (func).  Filter func for rcu_workload_free_callback and correlate
 * rhp to isolate this module's queue-to-invocation latency.
 *
 * Workload model:
 *   - A fixed number of mixed worker kthreads (closed loop).
 *   - A pre-generated, randomly shuffled operation block.
 *   - Every complete block has the exact configured read/write ratio.
 *   - The block repeats; concurrent workers may complete operations out of
 *     logical schedule order, which is normal for a concurrent workload.
 *   - A bounded callback backlog applies backpressure instead of permitting
 *     unbounded retired-object memory growth.
 */

#include <linux/atomic.h>
#include <linux/cpu.h>
#include <linux/delay.h>
#include <linux/errno.h>
#include <linux/kernel.h>
#include <linux/kthread.h>
#include <linux/list.h>
#include <linux/math64.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/rculist.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/timekeeping.h>
#include <linux/types.h>
#include <linux/vmalloc.h>
#include <linux/wait.h>

#define TEST_NAME                       "rcu_workload"
#define DEFAULT_SCHEDULE_OPS            65536U
#define MAX_SCHEDULE_OPS                1048576U
#define MAX_WORKER_THREADS              4096U
#define MAX_PENDING_CALLBACKS_LIMIT     1048576U

enum workload_op {
	WORKLOAD_READ = 0,
	WORKLOAD_WRITE = 1,
};

struct book {
	int id;
	char name[64];
	char author[64];
	int borrow;
	struct list_head node;
	struct rcu_head rcu;
};

struct workload_config {
	u32 read_weight;
	u32 write_weight;
	u32 worker_threads;
	u32 schedule_ops;
	u32 schedule_len;
	u32 schedule_reads;
	u32 schedule_writes;
	u32 schedule_seed;
	u32 run_seconds;
	u32 start_delay_ms;
	u32 reader_hold_us;
	u32 reader_delay_us;
	u32 writer_delay_us;
	u32 max_pending_callbacks;
	bool bind_threads;
	bool drain_callbacks;
};

static LIST_HEAD(books);
static DEFINE_MUTEX(update_mutex);
static DECLARE_WAIT_QUEUE_HEAD(control_wq);
static DECLARE_WAIT_QUEUE_HEAD(callback_wq);

static struct workload_config cfg;
static struct task_struct *controller_task;
static struct task_struct **worker_tasks;
static u8 *operation_schedule;

static bool workload_running;
static bool workload_stopping;
static bool workload_finished;

static u64 workload_start_ns;
static u64 workload_stop_ns;
static u64 callback_drain_end_ns;

static atomic64_t next_operation = ATOMIC64_INIT(0);
static atomic64_t read_claimed = ATOMIC64_INIT(0);
static atomic64_t write_claimed = ATOMIC64_INIT(0);
static atomic64_t read_completed = ATOMIC64_INIT(0);
static atomic64_t read_cancelled = ATOMIC64_INIT(0);
static atomic64_t write_attempted = ATOMIC64_INIT(0);
static atomic64_t write_completed = ATOMIC64_INIT(0);
static atomic64_t write_cancelled = ATOMIC64_INIT(0);
static atomic64_t write_alloc_failed = ATOMIC64_INIT(0);
static atomic64_t write_not_found = ATOMIC64_INIT(0);
static atomic64_t callbacks_queued = ATOMIC64_INIT(0);
static atomic64_t callbacks_invoked = ATOMIC64_INIT(0);
static atomic64_t callbacks_pending = ATOMIC64_INIT(0);
static atomic64_t callbacks_pending_max = ATOMIC64_INIT(0);
static atomic64_t callback_backpressure_ops = ATOMIC64_INIT(0);

static unsigned int read_weight = 99;
module_param(read_weight, uint, 0444);
MODULE_PARM_DESC(read_weight, "Read-operation weight; zero is allowed if write_weight is nonzero");

static unsigned int write_weight = 1;
module_param(write_weight, uint, 0444);
MODULE_PARM_DESC(write_weight, "Write-operation weight; zero is allowed if read_weight is nonzero");

static unsigned int worker_threads;
module_param(worker_threads, uint, 0444);
MODULE_PARM_DESC(worker_threads, "Mixed worker kthreads; zero means one per online CPU at module load");

static unsigned int schedule_ops = DEFAULT_SCHEDULE_OPS;
module_param(schedule_ops, uint, 0444);
MODULE_PARM_DESC(schedule_ops, "Minimum operations in the randomized exact-ratio schedule block");

static unsigned int schedule_seed = 1;
module_param(schedule_seed, uint, 0444);
MODULE_PARM_DESC(schedule_seed, "Deterministic shuffle seed; zero is replaced with a fixed nonzero seed");

static unsigned int run_seconds = 10;
module_param(run_seconds, uint, 0444);
MODULE_PARM_DESC(run_seconds, "Workload duration; zero means run until module unload");

static unsigned int start_delay_ms = 1000;
module_param(start_delay_ms, uint, 0444);
MODULE_PARM_DESC(start_delay_ms, "Delay before workers start, for attaching ftrace/bpftrace");

static unsigned int reader_hold_us;
module_param(reader_hold_us, uint, 0444);
MODULE_PARM_DESC(reader_hold_us, "Busy-hold duration inside each RCU read-side critical section");

static unsigned int reader_delay_us;
module_param(reader_delay_us, uint, 0444);
MODULE_PARM_DESC(reader_delay_us, "Stop-aware delay after each read operation");

static unsigned int writer_delay_us;
module_param(writer_delay_us, uint, 0444);
MODULE_PARM_DESC(writer_delay_us, "Stop-aware delay after each write operation");

static unsigned int max_pending_callbacks = 4096;
module_param(max_pending_callbacks, uint, 0444);
MODULE_PARM_DESC(max_pending_callbacks, "Maximum retired objects awaiting callback completion");

static bool bind_threads;
module_param(bind_threads, bool, 0444);
MODULE_PARM_DESC(bind_threads, "Bind workers round-robin to CPUs online at module load");

static bool drain_callbacks = true;
module_param(drain_callbacks, bool, 0444);
MODULE_PARM_DESC(drain_callbacks, "Run rcu_barrier() before printing the final summary");

/*
 * Keep this function visible and uninlined.  The core RCU tracepoints store
 * this exact function address in their func field.
 */
void rcu_workload_free_callback(struct rcu_head *head);

static u32 gcd_u32(u32 a, u32 b)
{
	while (b) {
		u32 remainder = a % b;

		a = b;
		b = remainder;
	}

	return a;
}

static u32 workload_prng_next(u32 *state)
{
	u32 value = *state;

	/* xorshift32 requires a nonzero state. */
	value ^= value << 13;
	value ^= value >> 17;
	value ^= value << 5;
	*state = value;
	return value;
}

static u32 workload_random_below(u32 *state, u32 upper_bound)
{
	/*
	 * Multiplication-based range reduction.  The schedule's read/write
	 * counts remain exact regardless of the permutation distribution.
	 */
	return (u32)(((u64)workload_prng_next(state) * upper_bound) >> 32);
}

static void atomic64_update_max(atomic64_t *maximum, s64 value)
{
	s64 observed = atomic64_read(maximum);

	while (value > observed) {
		s64 previous = atomic64_cmpxchg(maximum, observed, value);

		if (previous == observed)
			return;
		observed = previous;
	}
}

static int build_operation_schedule(void)
{
	u64 reduced_reads;
	u64 reduced_writes;
	u64 base_ops;
	u64 units;
	u64 length;
	u64 read_ops;
	u32 divisor;
	u32 state;
	u32 i;

	divisor = gcd_u32(cfg.read_weight, cfg.write_weight);
	if (!divisor)
		return -EINVAL;

	reduced_reads = cfg.read_weight / divisor;
	reduced_writes = cfg.write_weight / divisor;
	base_ops = reduced_reads + reduced_writes;
	units = DIV_ROUND_UP_ULL(cfg.schedule_ops, base_ops);
	if (!units)
		units = 1;

	if (base_ops > MAX_SCHEDULE_OPS ||
	    units > div64_u64(MAX_SCHEDULE_OPS, base_ops))
		return -E2BIG;

	length = base_ops * units;
	read_ops = reduced_reads * units;

	cfg.schedule_len = (u32)length;
	cfg.schedule_reads = (u32)read_ops;
	cfg.schedule_writes = cfg.schedule_len - cfg.schedule_reads;

	operation_schedule = kvmalloc_array(cfg.schedule_len,
					    sizeof(*operation_schedule),
					    GFP_KERNEL);
	if (!operation_schedule)
		return -ENOMEM;

	for (i = 0; i < cfg.schedule_reads; i++)
		operation_schedule[i] = WORKLOAD_READ;
	for (; i < cfg.schedule_len; i++)
		operation_schedule[i] = WORKLOAD_WRITE;

	state = cfg.schedule_seed;
	if (!state)
		state = 0x6d2b79f5U;
	cfg.schedule_seed = state;

	/* Fisher-Yates shuffle. */
	for (i = cfg.schedule_len; i > 1; i--) {
		u32 selected = workload_random_below(&state, i);
		u8 temporary = operation_schedule[i - 1];

		operation_schedule[i - 1] = operation_schedule[selected];
		operation_schedule[selected] = temporary;
	}

	return 0;
}

static int add_book(int id, const char *name, const char *author)
{
	struct book *book;

	book = kzalloc(sizeof(*book), GFP_KERNEL);
	if (!book)
		return -ENOMEM;

	book->id = id;
	strscpy(book->name, name, sizeof(book->name));
	strscpy(book->author, author, sizeof(book->author));
	book->borrow = 0;
	list_add_rcu(&book->node, &books);
	return 0;
}

static void free_current_books(void)
{
	struct book *book;
	struct book *temporary;

	/* All workers are stopped and all callbacks are drained before this. */
	mutex_lock(&update_mutex);
	list_for_each_entry_safe(book, temporary, &books, node) {
		list_del(&book->node);
		kfree(book);
	}
	mutex_unlock(&update_mutex);
}

static void request_workload_stop(void)
{
	WRITE_ONCE(workload_stopping, true);
	wake_up_all(&control_wq);
	wake_up_all(&callback_wq);
}

static bool workload_should_stop(void)
{
	return kthread_should_stop() || READ_ONCE(workload_stopping);
}

static void stop_aware_delay_us(u32 delay_us)
{
	ktime_t timeout;

	if (!delay_us)
		return;

	timeout = ns_to_ktime((u64)delay_us * NSEC_PER_USEC);
	wait_event_interruptible_hrtimeout(control_wq,
					 workload_should_stop(), timeout);
}

static void hold_rcu_reader(void)
{
	u64 end_ns;

	if (!cfg.reader_hold_us)
		return;

	end_ns = ktime_get_ns() + (u64)cfg.reader_hold_us * NSEC_PER_USEC;
	while (!workload_should_stop() && ktime_get_ns() < end_ns)
		cpu_relax();
}

static bool callback_capacity_available(void)
{
	return atomic64_read(&callbacks_pending) <
	       (s64)cfg.max_pending_callbacks;
}

static bool reserve_callback_slot(void)
{
	bool counted_backpressure = false;

	for (;;) {
		s64 pending;

		if (workload_should_stop())
			return false;

		pending = atomic64_read(&callbacks_pending);
		if (pending < (s64)cfg.max_pending_callbacks &&
		    atomic64_cmpxchg(&callbacks_pending, pending, pending + 1) ==
		    pending) {
			atomic64_update_max(&callbacks_pending_max, pending + 1);
			return true;
		}

		if (!counted_backpressure) {
			atomic64_inc(&callback_backpressure_ops);
			counted_backpressure = true;
		}

		wait_event_interruptible_exclusive(callback_wq,
						   workload_should_stop() ||
						   callback_capacity_available());
	}
}

static void release_callback_slot(void)
{
	s64 pending = atomic64_dec_return(&callbacks_pending);

	WARN_ON_ONCE(pending < 0);
	wake_up_interruptible(&callback_wq);
}

__visible noinline void rcu_workload_free_callback(struct rcu_head *head)
{
	struct book *book = container_of(head, struct book, rcu);

	kfree(book);
	atomic64_inc(&callbacks_invoked);
	release_callback_slot();
}

static noinline void rcu_workload_queue_callback(struct rcu_head *head)
{
	/* Increment first so a very fast callback cannot transiently pass it. */
	atomic64_inc(&callbacks_queued);
	call_rcu(head, rcu_workload_free_callback);
}

static int replace_book_state(int id)
{
	struct book *book;
	struct book *old_book = NULL;
	struct book *new_book;
	int result = 0;

	new_book = kzalloc(sizeof(*new_book), GFP_KERNEL);
	if (!new_book)
		return -ENOMEM;

	if (!reserve_callback_slot()) {
		kfree(new_book);
		return -ECANCELED;
	}

	mutex_lock(&update_mutex);
	list_for_each_entry(book, &books, node) {
		if (book->id == id) {
			old_book = book;
			break;
		}
	}

	if (!old_book) {
		result = -ENOENT;
		goto out_unlock;
	}

	/* Copy payload only; never copy list_head or rcu_head state. */
	new_book->id = old_book->id;
	strscpy(new_book->name, old_book->name, sizeof(new_book->name));
	strscpy(new_book->author, old_book->author,
		 sizeof(new_book->author));
	new_book->borrow = !old_book->borrow;

	list_replace_rcu(&old_book->node, &new_book->node);
	mutex_unlock(&update_mutex);

	rcu_workload_queue_callback(&old_book->rcu);
	return 0;

out_unlock:
	mutex_unlock(&update_mutex);
	release_callback_slot();
	kfree(new_book);
	return result;
}

static void perform_read_operation(void)
{
	struct book *book;
	int observed = 0;

	rcu_read_lock();
	list_for_each_entry_rcu(book, &books, node)
		observed += READ_ONCE(book->borrow);
	hold_rcu_reader();
	rcu_read_unlock();

	/* Keep the traversal's load observable without logging in the hot path. */
	if (unlikely(observed < 0))
		pr_warn_ratelimited(TEST_NAME ": impossible read result=%d\n",
				    observed);

	atomic64_inc(&read_completed);
}

static void perform_write_operation(void)
{
	int result;

	atomic64_inc(&write_attempted);
	result = replace_book_state(0);
	if (!result) {
		atomic64_inc(&write_completed);
		return;
	}

	if (result == -ECANCELED)
		atomic64_inc(&write_cancelled);
	else if (result == -ENOMEM)
		atomic64_inc(&write_alloc_failed);
	else if (result == -ENOENT)
		atomic64_inc(&write_not_found);
}

static enum workload_op claim_next_operation(void)
{
	u64 sequence = atomic64_fetch_inc(&next_operation);
	u32 index = do_div(sequence, cfg.schedule_len);
	enum workload_op operation = operation_schedule[index];

	if (operation == WORKLOAD_READ)
		atomic64_inc(&read_claimed);
	else
		atomic64_inc(&write_claimed);

	return operation;
}

static int workload_worker(void *argument)
{
	unsigned int worker_id = (unsigned long)argument;

	wait_event_interruptible(control_wq,
				 kthread_should_stop() ||
				 READ_ONCE(workload_running) ||
				 READ_ONCE(workload_stopping));

	while (!workload_should_stop()) {
		enum workload_op operation = claim_next_operation();

		if (unlikely(workload_should_stop())) {
			if (operation == WORKLOAD_READ)
				atomic64_inc(&read_cancelled);
			else
				atomic64_inc(&write_cancelled);
			break;
		}

		if (operation == WORKLOAD_READ) {
			perform_read_operation();
			stop_aware_delay_us(cfg.reader_delay_us);
		} else {
			perform_write_operation();
			stop_aware_delay_us(cfg.writer_delay_us);
		}

		if (!cfg.reader_delay_us && !cfg.writer_delay_us)
			cond_resched();
	}

	pr_debug(TEST_NAME ": worker=%u stopped\n", worker_id);
	return 0;
}

static int nth_online_cpu(unsigned int index)
{
	unsigned int cpu;
	unsigned int ordinal = 0;

	for_each_online_cpu(cpu) {
		if (ordinal++ == index)
			return cpu;
	}

	return -ENODEV;
}

static void stop_worker_tasks(void)
{
	unsigned int i;

	request_workload_stop();

	if (!worker_tasks)
		return;

	for (i = 0; i < cfg.worker_threads; i++) {
		if (worker_tasks[i]) {
			kthread_stop(worker_tasks[i]);
			worker_tasks[i] = NULL;
		}
	}

	kfree(worker_tasks);
	worker_tasks = NULL;
}

static int create_worker_tasks(void)
{
	unsigned int online_cpus;
	unsigned int i;
	int result = 0;

	worker_tasks = kcalloc(cfg.worker_threads, sizeof(*worker_tasks),
			       GFP_KERNEL);
	if (!worker_tasks)
		return -ENOMEM;

	cpus_read_lock();
	online_cpus = num_online_cpus();
	if (!online_cpus) {
		result = -ENODEV;
		goto out_unlock_cpus;
	}

	for (i = 0; i < cfg.worker_threads; i++) {
		struct task_struct *task;

		task = kthread_create(workload_worker,
				      (void *)(unsigned long)i,
				      "rcu_mix/%u", i);
		if (IS_ERR(task)) {
			result = PTR_ERR(task);
			goto out_unlock_cpus;
		}

		if (cfg.bind_threads) {
			int cpu = nth_online_cpu(i % online_cpus);

			if (cpu < 0) {
				kthread_stop(task);
				result = cpu;
				goto out_unlock_cpus;
			}
			kthread_bind(task, cpu);
		}

		worker_tasks[i] = task;
		wake_up_process(task);
	}

out_unlock_cpus:
	cpus_read_unlock();
	if (result)
		stop_worker_tasks();
	return result;
}

static bool wait_until_or_stop(u64 deadline_ns)
{
	for (;;) {
		u64 now_ns;
		ktime_t timeout;

		if (workload_should_stop())
			return true;

		now_ns = ktime_get_ns();
		if (now_ns >= deadline_ns)
			return false;

		timeout = ns_to_ktime(deadline_ns - now_ns);
		wait_event_interruptible_hrtimeout(control_wq,
					     workload_should_stop(), timeout);
	}
}

static u64 ratio_x1000(u64 numerator, u64 denominator)
{
	u64 whole;
	u64 remainder;

	if (!denominator)
		return 0;

	whole = div64_u64_rem(numerator, denominator, &remainder);
	return whole * 1000 + div64_u64(remainder * 1000, denominator);
}

static void print_summary(void)
{
	u64 claimed_reads = atomic64_read(&read_claimed);
	u64 claimed_writes = atomic64_read(&write_claimed);
	u64 completed_reads = atomic64_read(&read_completed);
	u64 completed_writes = atomic64_read(&write_completed);
	u64 claimed_total = claimed_reads + claimed_writes;
	u64 full_blocks = div64_u64(claimed_total, cfg.schedule_len);
	u64 partial_ops = claimed_total - full_blocks * cfg.schedule_len;
	u64 workload_ns = workload_stop_ns > workload_start_ns ?
		workload_stop_ns - workload_start_ns : 0;
	u64 drain_ns = callback_drain_end_ns > workload_stop_ns ?
		callback_drain_end_ns - workload_stop_ns : 0;

	pr_info(TEST_NAME ": summary model=closed_loop_shuffled "
		"read_weight=%u write_weight=%u workers=%u bind=%u "
		"schedule_len=%u schedule_reads=%u schedule_writes=%u seed=%u "
		"claimed_reads=%llu claimed_writes=%llu claimed_ratio_x1000=%llu "
		"completed_reads=%llu completed_writes=%llu completed_ratio_x1000=%llu "
		"read_cancelled=%lld write_attempted=%lld write_cancelled=%lld "
		"write_alloc_failed=%lld write_not_found=%lld full_blocks=%llu "
		"partial_ops=%llu callbacks_queued=%lld callbacks_invoked=%lld "
		"callbacks_pending=%lld callbacks_pending_max=%lld "
		"callback_backpressure_ops=%lld workload_ms=%llu callback_drain_ms=%llu "
		"reader_hold_us=%u reader_delay_us=%u writer_delay_us=%u "
		"drain_callbacks=%u\n",
		cfg.read_weight, cfg.write_weight, cfg.worker_threads,
		cfg.bind_threads, cfg.schedule_len, cfg.schedule_reads,
		cfg.schedule_writes, cfg.schedule_seed,
		(unsigned long long)claimed_reads,
		(unsigned long long)claimed_writes,
		(unsigned long long)ratio_x1000(claimed_reads, claimed_writes),
		(unsigned long long)completed_reads,
		(unsigned long long)completed_writes,
		(unsigned long long)ratio_x1000(completed_reads, completed_writes),
		(long long)atomic64_read(&read_cancelled),
		(long long)atomic64_read(&write_attempted),
		(long long)atomic64_read(&write_cancelled),
		(long long)atomic64_read(&write_alloc_failed),
		(long long)atomic64_read(&write_not_found),
		(unsigned long long)full_blocks,
		(unsigned long long)partial_ops,
		(long long)atomic64_read(&callbacks_queued),
		(long long)atomic64_read(&callbacks_invoked),
		(long long)atomic64_read(&callbacks_pending),
		(long long)atomic64_read(&callbacks_pending_max),
		(long long)atomic64_read(&callback_backpressure_ops),
		(unsigned long long)div_u64(workload_ns, NSEC_PER_MSEC),
		(unsigned long long)div_u64(drain_ns, NSEC_PER_MSEC),
		cfg.reader_hold_us, cfg.reader_delay_us, cfg.writer_delay_us,
		cfg.drain_callbacks);
}

static int controller_thread(void *unused)
{
	u64 start_deadline_ns;

	start_deadline_ns = ktime_get_ns() +
			    (u64)cfg.start_delay_ms * NSEC_PER_MSEC;
	if (wait_until_or_stop(start_deadline_ns))
		goto stop_and_drain;

	workload_start_ns = ktime_get_ns();
	WRITE_ONCE(workload_running, true);
	wake_up_all(&control_wq);

	pr_info(TEST_NAME ": started model=closed_loop_shuffled "
		"read_weight=%u write_weight=%u workers=%u schedule_len=%u "
		"run_seconds=%u max_pending_callbacks=%u "
		"callback_func=rcu_workload_free_callback\n",
		cfg.read_weight, cfg.write_weight, cfg.worker_threads,
		cfg.schedule_len, cfg.run_seconds, cfg.max_pending_callbacks);

	if (cfg.run_seconds) {
		u64 stop_deadline_ns = workload_start_ns +
				       (u64)cfg.run_seconds * NSEC_PER_SEC;

		wait_until_or_stop(stop_deadline_ns);
	} else {
		wait_event_interruptible(control_wq, workload_should_stop());
	}

stop_and_drain:
	request_workload_stop();
	workload_stop_ns = ktime_get_ns();
	stop_worker_tasks();

	if (cfg.drain_callbacks)
		rcu_barrier();
	callback_drain_end_ns = ktime_get_ns();

	print_summary();
	WRITE_ONCE(workload_finished, true);
	pr_info(TEST_NAME ": done callbacks_drained=%u\n",
		cfg.drain_callbacks);

	/* Keep the module loaded without spinning until rmmod calls kthread_stop(). */
	while (!kthread_should_stop())
		wait_event_interruptible(control_wq, kthread_should_stop());

	return 0;
}

static int validate_and_snapshot_config(void)
{
	u32 online_cpus;

	if (!read_weight && !write_weight) {
		pr_err(TEST_NAME ": read_weight and write_weight cannot both be zero\n");
		return -EINVAL;
	}

	if (!schedule_ops || schedule_ops > MAX_SCHEDULE_OPS) {
		pr_err(TEST_NAME ": schedule_ops must be in [1, %u]\n",
		       MAX_SCHEDULE_OPS);
		return -EINVAL;
	}

	if (!max_pending_callbacks ||
	    max_pending_callbacks > MAX_PENDING_CALLBACKS_LIMIT) {
		pr_err(TEST_NAME ": max_pending_callbacks must be in [1, %u]\n",
		       MAX_PENDING_CALLBACKS_LIMIT);
		return -EINVAL;
	}

	cpus_read_lock();
	online_cpus = num_online_cpus();
	cpus_read_unlock();
	if (!online_cpus)
		return -ENODEV;

	cfg.read_weight = read_weight;
	cfg.write_weight = write_weight;
	cfg.worker_threads = worker_threads ? worker_threads : online_cpus;
	if (!cfg.worker_threads || cfg.worker_threads > MAX_WORKER_THREADS) {
		pr_err(TEST_NAME ": worker_threads must be in [1, %u]\n",
		       MAX_WORKER_THREADS);
		return -EINVAL;
	}

	cfg.schedule_ops = schedule_ops;
	cfg.schedule_seed = schedule_seed;
	cfg.run_seconds = run_seconds;
	cfg.start_delay_ms = start_delay_ms;
	cfg.reader_hold_us = reader_hold_us;
	cfg.reader_delay_us = reader_delay_us;
	cfg.writer_delay_us = writer_delay_us;
	cfg.max_pending_callbacks = max_pending_callbacks;
	cfg.bind_threads = bind_threads;
	cfg.drain_callbacks = drain_callbacks;
	return 0;
}

static int __init test_rcu_init(void)
{
	int result;

	result = validate_and_snapshot_config();
	if (result)
		return result;

	result = build_operation_schedule();
	if (result) {
		if (result == -E2BIG)
			pr_err(TEST_NAME ": reduced ratio requires a block larger than %u operations\n",
			       MAX_SCHEDULE_OPS);
		return result;
	}

	result = add_book(0, "book1", "jb");
	if (result)
		goto free_schedule;

	result = create_worker_tasks();
	if (result)
		goto free_books;

	controller_task = kthread_run(controller_thread, NULL,
				      "rcu_workload_ctl");
	if (IS_ERR(controller_task)) {
		result = PTR_ERR(controller_task);
		controller_task = NULL;
		stop_worker_tasks();
		goto free_books;
	}

	pr_info(TEST_NAME ": loaded; attach tracing during start_delay_ms=%u\n",
		cfg.start_delay_ms);
	return 0;

free_books:
	/* No callbacks exist on these init-failure paths. */
	free_current_books();
free_schedule:
	kvfree(operation_schedule);
	operation_schedule = NULL;
	return result;
}

static void __exit test_rcu_exit(void)
{
	request_workload_stop();

	if (controller_task) {
		kthread_stop(controller_task);
		controller_task = NULL;
	} else {
		stop_worker_tasks();
	}

	/*
	 * Required before unloading a module that supplied call_rcu() callback
	 * functions.  This is also required when drain_callbacks=0.
	 */
	rcu_barrier();
	free_current_books();
	kvfree(operation_schedule);
	operation_schedule = NULL;

	pr_info(TEST_NAME ": unloaded finished=%u\n",
		READ_ONCE(workload_finished));
}

module_init(test_rcu_init);
module_exit(test_rcu_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("sbcd90; revised RCU callback workload generator");
MODULE_DESCRIPTION("Exact-ratio shuffled RCU list workload using traceable call_rcu callbacks");
