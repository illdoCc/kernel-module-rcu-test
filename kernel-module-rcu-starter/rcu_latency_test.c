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
#include <linux/spinlock.h>
#include <linux/string.h>
#include <linux/timekeeping.h>

struct book {
	int id;
	char name[64];
	char author[64];
	int borrow;
	struct list_head node;
	struct rcu_head rcu;
};

static LIST_HEAD(books);
static spinlock_t books_lock;
static DEFINE_MUTEX(update_mutex);
static DEFINE_SPINLOCK(stats_lock);
static struct task_struct *controller_task;
static struct task_struct **reader_tasks;
static struct task_struct **writer_tasks;

struct latency_stats {
	u64 ops;
	u64 total_us;
	u64 min_us;
	u64 max_us;
};

static struct latency_stats normal_stats;
static struct latency_stats expedited_stats;
static atomic64_t reader_ops;

static unsigned int readers = 1;
module_param(readers, uint, 0644);
MODULE_PARM_DESC(readers, "Number of reader kthreads");

static unsigned int writers = 1;
module_param(writers, uint, 0644);
MODULE_PARM_DESC(writers, "Number of writer kthreads");

static unsigned int run_seconds = 10;
module_param(run_seconds, uint, 0644);
MODULE_PARM_DESC(run_seconds, "Workload duration in seconds; 0 means run until module unload");

static unsigned int reader_hold_us;
module_param(reader_hold_us, uint, 0644);
MODULE_PARM_DESC(reader_hold_us, "Microseconds each reader holds rcu_read_lock()");

static unsigned int reader_delay_us;
module_param(reader_delay_us, uint, 0644);
MODULE_PARM_DESC(reader_delay_us, "Delay between reader iterations in microseconds");

static unsigned int writer_delay_ms = 250;
module_param(writer_delay_ms, uint, 0644);
MODULE_PARM_DESC(writer_delay_ms, "Delay between writer iterations in milliseconds");

static unsigned int start_delay_ms = 1000;
module_param(start_delay_ms, uint, 0644);
MODULE_PARM_DESC(start_delay_ms, "Delay before the test starts, useful for attaching bpftrace");

static char *mode = "both";
module_param(mode, charp, 0644);
MODULE_PARM_DESC(mode, "Test mode: normal, expedited, or both");

static bool verbose;
module_param(verbose, bool, 0644);
MODULE_PARM_DESC(verbose, "Log every writer update latency when true");

static bool want_normal(void)
{
	return !strcmp(mode, "normal") || !strcmp(mode, "both");
}

static bool want_expedited(void)
{
	return !strcmp(mode, "expedited") || !strcmp(mode, "both");
}

static bool valid_mode(void)
{
	return want_normal() || want_expedited();
}

static int add_book(int id, const char *name, const char *author)
{
	struct book *bk;

	bk = kzalloc(sizeof(*bk), GFP_KERNEL);
	if (!bk)
		return -ENOMEM;

	bk->id = id;
	strscpy(bk->name, name, sizeof(bk->name));
	strscpy(bk->author, author, sizeof(bk->author));
	bk->borrow = 0;

	spin_lock(&books_lock);
	list_add_rcu(&bk->node, &books);
	spin_unlock(&books_lock);

	return 0;
}

static void print_book(int id)
{
	struct book *bk;

	rcu_read_lock();
	list_for_each_entry_rcu(bk, &books, node) {
		if (bk->id == id) {
			pr_info("rcu_latency_test: book id=%d name=%s borrow=%d addr=%px\n",
				bk->id, bk->name, bk->borrow, bk);
			rcu_read_unlock();
			return;
		}
	}
	rcu_read_unlock();

	pr_info("rcu_latency_test: book id=%d not found\n", id);
}

static void record_latency(bool expedited, u64 elapsed_us)
{
	struct latency_stats *stats;
	unsigned long flags;

	stats = expedited ? &expedited_stats : &normal_stats;

	spin_lock_irqsave(&stats_lock, flags);
	if (!stats->ops || elapsed_us < stats->min_us)
		stats->min_us = elapsed_us;
	if (elapsed_us > stats->max_us)
		stats->max_us = elapsed_us;
	stats->ops++;
	stats->total_us += elapsed_us;
	spin_unlock_irqrestore(&stats_lock, flags);
}

static void print_one_summary(const char *name, const struct latency_stats *src)
{
	struct latency_stats stats;
	unsigned long flags;
	u64 avg_us = 0;

	spin_lock_irqsave(&stats_lock, flags);
	stats = *src;
	spin_unlock_irqrestore(&stats_lock, flags);

	if (stats.ops)
		avg_us = div64_u64(stats.total_us, stats.ops);

	pr_info("rcu_latency_test: summary mode=%s writer_ops=%llu avg_us=%llu min_us=%llu max_us=%llu\n",
		name, stats.ops, avg_us, stats.min_us, stats.max_us);
}

static void print_summary(void)
{
	pr_info("rcu_latency_test: summary readers=%u writers=%u ratio=%u:%u reader_ops=%lld reader_hold_us=%u reader_delay_us=%u writer_delay_ms=%u run_seconds=%u mode=%s\n",
		readers, writers, readers, writers, atomic64_read(&reader_ops),
		reader_hold_us, reader_delay_us, writer_delay_ms, run_seconds, mode);

	if (want_normal())
		print_one_summary("normal", &normal_stats);
	if (want_expedited())
		print_one_summary("expedited", &expedited_stats);
}

static void hold_reader_lock(void)
{
	u64 end_ns;

	if (!reader_hold_us)
		return;

	end_ns = ktime_get_ns() + (u64)reader_hold_us * 1000;
	while (!kthread_should_stop() && ktime_get_ns() < end_ns)
		cpu_relax();
}

static int replace_book_state(int id, int borrow, bool expedited,
			      unsigned int writer_id, unsigned int iter)
{
	struct book *bk;
	struct book *old_bk = NULL;
	struct book *new_bk;
	const char *rcu_mode = expedited ? "expedited" : "normal";
	u64 start_ns;
	u64 elapsed_us;
	int ret = 0;

	mutex_lock(&update_mutex);

	rcu_read_lock();
	list_for_each_entry_rcu(bk, &books, node) {
		if (bk->id == id) {
			old_bk = bk;
			break;
		}
	}

	if (!old_bk) {
		rcu_read_unlock();
		ret = -ENOENT;
		goto out_unlock_update;
	}

	new_bk = kzalloc(sizeof(*new_bk), GFP_ATOMIC);
	if (!new_bk) {
		rcu_read_unlock();
		ret = -ENOMEM;
		goto out_unlock_update;
	}

	memcpy(new_bk, old_bk, sizeof(*new_bk));
	new_bk->borrow = borrow;

	spin_lock(&books_lock);
	list_replace_rcu(&old_bk->node, &new_bk->node);
	spin_unlock(&books_lock);
	rcu_read_unlock();

	if (verbose)
		pr_info("rcu_latency_test: begin mode=%s writer=%u iter=%u action=%s\n",
			rcu_mode, writer_id, iter, borrow ? "borrow" : "return");

	start_ns = ktime_get_ns();
	if (expedited)
		synchronize_rcu_expedited();
	else
		synchronize_rcu();
	elapsed_us = div_u64(ktime_get_ns() - start_ns, 1000);
	record_latency(expedited, elapsed_us);

	if (verbose)
		pr_info("rcu_latency_test: end mode=%s writer=%u iter=%u action=%s latency_us=%llu\n",
			rcu_mode, writer_id, iter, borrow ? "borrow" : "return",
			elapsed_us);

	kfree(old_bk);

out_unlock_update:
	mutex_unlock(&update_mutex);
	return ret;
}

static void free_books(void)
{
	struct book *bk;
	struct book *tmp;

	spin_lock(&books_lock);
	list_for_each_entry_safe(bk, tmp, &books, node) {
		list_del_rcu(&bk->node);
		kfree(bk);
	}
	spin_unlock(&books_lock);
}

static int reader_thread(void *arg)
{
	unsigned int id = (unsigned long)arg;
	struct book *bk;
	int seen;

	while (!kthread_should_stop()) {
		seen = 0;

		rcu_read_lock();
		list_for_each_entry_rcu(bk, &books, node)
			seen += READ_ONCE(bk->borrow);
		hold_reader_lock();
		rcu_read_unlock();

		atomic64_inc(&reader_ops);

		if (unlikely(seen < 0))
			pr_info("rcu_latency_test: reader=%u impossible seen=%d\n",
				id, seen);

		if (reader_delay_us)
			usleep_range(reader_delay_us, reader_delay_us + 100);
		else
			cond_resched();
	}

	return 0;
}

static int writer_thread(void *arg)
{
	unsigned int id = (unsigned long)arg;
	unsigned int iter = 0;
	int borrow = 1;

	while (!kthread_should_stop()) {
		if (want_normal()) {
			replace_book_state(0, borrow, false, id, iter);
			borrow = !borrow;
		}

		if (kthread_should_stop())
			break;

		if (want_expedited()) {
			replace_book_state(0, borrow, true, id, iter);
			borrow = !borrow;
		}

		iter++;

		if (writer_delay_ms)
			msleep(writer_delay_ms);
		else
			cond_resched();
	}

	return 0;
}

static void stop_workers(void)
{
	unsigned int i;

	if (reader_tasks) {
		for (i = 0; i < readers; i++) {
			if (reader_tasks[i])
				kthread_stop(reader_tasks[i]);
		}
		kfree(reader_tasks);
		reader_tasks = NULL;
	}

	if (writer_tasks) {
		for (i = 0; i < writers; i++) {
			if (writer_tasks[i])
				kthread_stop(writer_tasks[i]);
		}
		kfree(writer_tasks);
		writer_tasks = NULL;
	}
}

static int start_workers(void)
{
	unsigned int i;
	int ret;

	reader_tasks = kcalloc(readers, sizeof(*reader_tasks), GFP_KERNEL);
	if (!reader_tasks)
		return -ENOMEM;

	writer_tasks = kcalloc(writers, sizeof(*writer_tasks), GFP_KERNEL);
	if (!writer_tasks) {
		ret = -ENOMEM;
		goto err_stop;
	}

	for (i = 0; i < readers; i++) {
		reader_tasks[i] = kthread_run(reader_thread, (void *)(unsigned long)i,
					      "rcu_reader/%u", i);
		if (IS_ERR(reader_tasks[i])) {
			ret = PTR_ERR(reader_tasks[i]);
			reader_tasks[i] = NULL;
			goto err_stop;
		}
	}

	for (i = 0; i < writers; i++) {
		writer_tasks[i] = kthread_run(writer_thread, (void *)(unsigned long)i,
					      "rcu_writer/%u", i);
		if (IS_ERR(writer_tasks[i])) {
			ret = PTR_ERR(writer_tasks[i]);
			writer_tasks[i] = NULL;
			goto err_stop;
		}
	}

	return 0;

err_stop:
	stop_workers();
	return ret;
}

static bool wait_or_stop_ms(unsigned int total_ms)
{
	unsigned int slept = 0;
	unsigned int step;

	while (!kthread_should_stop() && slept < total_ms) {
		step = min(250U, total_ms - slept);
		msleep(step);
		slept += step;
	}

	return kthread_should_stop();
}

static int run_book_rcu_test(void *arg)
{
	int ret;

	if (wait_or_stop_ms(start_delay_ms))
		return 0;

	pr_info("rcu_latency_test: start readers=%u writers=%u ratio=%u:%u run_seconds=%u reader_hold_us=%u reader_delay_us=%u writer_delay_ms=%u mode=%s verbose=%d\n",
		readers, writers, readers, writers, run_seconds, reader_hold_us,
		reader_delay_us, writer_delay_ms, mode, verbose);
	print_book(0);

	ret = start_workers();
	if (ret) {
		pr_err("rcu_latency_test: failed to start workers ret=%d\n", ret);
		goto wait_for_unload;
	}

	if (run_seconds) {
		wait_or_stop_ms(run_seconds * 1000);
	} else {
		while (!kthread_should_stop())
			msleep(250);
	}

	stop_workers();
	print_book(0);
	print_summary();
	pr_info("rcu_latency_test: done\n");

wait_for_unload:
	while (!kthread_should_stop())
		msleep(250);

	return 0;
}

static int __init rcu_latency_test_init(void)
{
	int ret;

	if (!readers || !writers) {
		pr_err("rcu_latency_test: readers and writers must both be greater than 0\n");
		return -EINVAL;
	}

	if (!valid_mode()) {
		pr_err("rcu_latency_test: invalid mode=%s, use normal, expedited, or both\n",
		       mode);
		return -EINVAL;
	}

	spin_lock_init(&books_lock);

	ret = add_book(0, "book1", "jb");
	if (ret)
		return ret;

	controller_task = kthread_run(run_book_rcu_test, NULL, "rcu_book_test");
	if (IS_ERR(controller_task)) {
		ret = PTR_ERR(controller_task);
		controller_task = NULL;
		free_books();
		return ret;
	}

	return 0;
}

static void __exit rcu_latency_test_exit(void)
{
	if (controller_task)
		kthread_stop(controller_task);

	free_books();
	pr_info("rcu_latency_test: unloaded\n");
}

module_init(rcu_latency_test_init);
module_exit(rcu_latency_test_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("kernel-module-rcu-test");
MODULE_DESCRIPTION("Book-list RCU workload test with configurable reader:writer ratio");
