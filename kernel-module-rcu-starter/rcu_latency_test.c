#include <linux/delay.h>
#include <linux/kernel.h>
#include <linux/kthread.h>
#include <linux/list.h>
#include <linux/module.h>
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
static struct task_struct *test_task;

static unsigned int iterations = 20;
module_param(iterations, uint, 0644);
MODULE_PARM_DESC(iterations, "Number of borrow/return RCU updates per selected mode");

static unsigned int delay_ms = 250;
module_param(delay_ms, uint, 0644);
MODULE_PARM_DESC(delay_ms, "Delay between test iterations in milliseconds");

static unsigned int start_delay_ms = 1000;
module_param(start_delay_ms, uint, 0644);
MODULE_PARM_DESC(start_delay_ms, "Delay before the test starts, useful for attaching bpftrace");

static char *mode = "both";
module_param(mode, charp, 0644);
MODULE_PARM_DESC(mode, "Test mode: normal, expedited, or both");

static bool want_normal(void)
{
	return !strcmp(mode, "normal") || !strcmp(mode, "both");
}

static bool want_expedited(void)
{
	return !strcmp(mode, "expedited") || !strcmp(mode, "both");
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

static int replace_book_state(int id, int borrow, bool expedited, unsigned int iter)
{
	struct book *bk;
	struct book *old_bk = NULL;
	struct book *new_bk;
	const char *rcu_mode = expedited ? "expedited" : "normal";
	u64 start_ns;
	u64 elapsed_us;

	rcu_read_lock();
	list_for_each_entry_rcu(bk, &books, node) {
		if (bk->id == id) {
			old_bk = bk;
			break;
		}
	}

	if (!old_bk) {
		rcu_read_unlock();
		return -ENOENT;
	}

	new_bk = kzalloc(sizeof(*new_bk), GFP_ATOMIC);
	if (!new_bk) {
		rcu_read_unlock();
		return -ENOMEM;
	}

	memcpy(new_bk, old_bk, sizeof(*new_bk));
	new_bk->borrow = borrow;

	spin_lock(&books_lock);
	list_replace_rcu(&old_bk->node, &new_bk->node);
	spin_unlock(&books_lock);
	rcu_read_unlock();

	pr_info("rcu_latency_test: begin mode=%s iter=%u action=%s\n",
		rcu_mode, iter, borrow ? "borrow" : "return");

	start_ns = ktime_get_ns();
	if (expedited)
		synchronize_rcu_expedited();
	else
		synchronize_rcu();
	elapsed_us = div_u64(ktime_get_ns() - start_ns, 1000);

	pr_info("rcu_latency_test: end mode=%s iter=%u action=%s latency_us=%llu\n",
		rcu_mode, iter, borrow ? "borrow" : "return", elapsed_us);

	kfree(old_bk);
	return 0;
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

static int run_book_rcu_test(void *arg)
{
	unsigned int i;

	if (!want_normal() && !want_expedited()) {
		pr_err("rcu_latency_test: invalid mode=%s, use normal, expedited, or both\n",
		       mode);
		return -EINVAL;
	}

	msleep(start_delay_ms);

	pr_info("rcu_latency_test: start iterations=%u delay_ms=%u mode=%s\n",
		iterations, delay_ms, mode);
	print_book(0);

	for (i = 0; i < iterations && !kthread_should_stop(); i++) {
		if (want_normal()) {
			replace_book_state(0, 1, false, i);
			replace_book_state(0, 0, false, i);
		}

		if (kthread_should_stop())
			break;

		if (want_expedited()) {
			replace_book_state(0, 1, true, i);
			replace_book_state(0, 0, true, i);
		}

		if (delay_ms)
			msleep(delay_ms);
	}

	print_book(0);
	pr_info("rcu_latency_test: done\n");
	return 0;
}

static int __init rcu_latency_test_init(void)
{
	int ret;

	spin_lock_init(&books_lock);

	ret = add_book(0, "book1", "jb");
	if (ret)
		return ret;

	test_task = kthread_run(run_book_rcu_test, NULL, "rcu_book_test");
	if (IS_ERR(test_task)) {
		ret = PTR_ERR(test_task);
		test_task = NULL;
		free_books();
		return ret;
	}

	return 0;
}

static void __exit rcu_latency_test_exit(void)
{
	if (test_task)
		kthread_stop(test_task);

	free_books();
	pr_info("rcu_latency_test: unloaded\n");
}

module_init(rcu_latency_test_init);
module_exit(rcu_latency_test_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("kernel-module-rcu-test");
MODULE_DESCRIPTION("Book-list RCU test for normal and expedited grace periods");
