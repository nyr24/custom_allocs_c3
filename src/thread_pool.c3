module data_structures::thread_pool;

import std::thread;

const THREAD_TEMP_MEM_AMOUNT = 1 << 18; // 262kb

struct ThreadTask
{
	ThreadFn func;
	void* arg;
}

<*
 User should fill this structure to submit multiple tasks.
*>
struct ThreadTaskPack
{
	TaskPriority priority;
	ThreadTask[] tasks;
}

<*
 Collects all the data pool needs for synchronization.
 Only for internal use.
*>
struct PoolData @private
{
	Mutex mutex;
	ConditionVariable cond_task_acquired;
	ConditionVariable cond_task_finished;
	ConditionVariable cond_collect;
	bitstruct : sz
	{
		bool initialized : 0..0;
		bool stop : 1..1;
		bool stop_now : 2..2;
		sz alive_thread_count : 3..(bitsizeof(sz) - 1);
	}
}

<*
 Task priority.
 LOW - For IO / background tasks.
 HIGH - For computation-intensive, urgent tasks.
*>
enum TaskPriority : (ThreadPriority thread_prio)
{
	// For IO / background tasks.
	LOW  { DEFAULT  },
	// For computation-intensive, urgent tasks.
	HIGH { REALTIME },
}

alias ThreadFn = fn void(void* task_data);

<*
 @require HIGH_THREADS > 0 && LOW_THREADS > 0 && HIGH_TASKS > 0 && LOW_TASKS > 0 : "Should be at least 1"
*>
module data_structures::thread_pool <HIGH_THREADS, LOW_THREADS, HIGH_TASKS, LOW_TASKS>;
import std::thread @norecurse;
import std::math @norecurse;
import std::collections::ringbuffer;

<*
 Thread pool root structure which manages 2 separate pools: high priority, and low priority.
*>
struct ThreadPool
{
	PoolData high_pool;
	PoolData low_pool;
	RingBuffer{ThreadTask, HIGH_TASKS} high_queue;
	RingBuffer{ThreadTask, LOW_TASKS} low_queue;
	Thread[HIGH_THREADS] high_threads;
	Thread[LOW_THREADS] low_threads;
	bool[<HIGH_THREADS>] high_threads_working;
	bool[<LOW_THREADS>] low_threads_working;
}

<*
 Passed to the thread upon creation.
*>
struct ThreadCreateData @local
{
	ThreadPool* tpool;
	// Corresponds to '_threads_working' array
	sz index;
	Allocator mem;
	Allocator tmem;
}

<*
 @require !self.is_initialized() : "Pools must not be already initialized"
*>
fn void? ThreadPool.init(&self, Allocator thread_main_alloc = mem, Allocator thread_temp_alloc = tmem, sz stack_size_low = 0, sz stack_size_high = 0)
{
	defer catch self.destroy();

	PoolData* pool = &self.high_pool;
	pool.alive_thread_count = HIGH_THREADS;
	pool.initialized = true;
	pool.mutex.init()!;
	pool.cond_task_acquired.init()!;
	pool.cond_task_finished.init()!;
	pool.cond_collect.init()!;

	pool = &self.low_pool;
	pool.alive_thread_count = LOW_THREADS;
	pool.initialized = true;
	pool.mutex.init()!;
	pool.cond_task_acquired.init()!;
	pool.cond_task_finished.init()!;
	pool.cond_collect.init()!;

	self.high_queue.init();
	self.low_queue.init();

	self.high_threads_working = {};
	self.low_threads_working = {};

	foreach (i, &thread : self.high_threads)
	{
		ThreadCreateData thread_data = { self, i, thread_main_alloc, thread_temp_alloc };
		thread.create(&process_high_prio_task, &thread_data, { stack_size_high, TaskPriority.HIGH.thread_prio })!;
		// The thread resources will be cleaned up when the thread exits.
		thread.detach()!;
	}
	
	foreach (i, &thread : self.low_threads)
	{
		ThreadCreateData thread_data = { self, i, thread_main_alloc, thread_temp_alloc };
		thread.create(&process_low_prio_task, &thread_data, { stack_size_low, TaskPriority.LOW.thread_prio })!;
		// The thread resources will be cleaned up when the thread exits.
		thread.detach()!;
	}
}

<*
 Awaits all tasks in both queues.

 @require self.is_initialized() : "Pools must be already initialized"
*>
fn void ThreadPool.await_all(&self)
{
	PoolData* pool = &self.high_pool;

	{
		pool.mutex.lock();
		defer pool.mutex.unlock();
		while (!self.high_queue.is_empty() || self.high_threads_working.or())
		{
			pool.cond_task_finished.wait(&pool.mutex);
		}
	}

	pool = &self.low_pool;

	{
		pool.mutex.lock();
		defer pool.mutex.unlock();
		while (!self.low_queue.is_empty() || self.low_threads_working.or())
		{
			pool.cond_task_finished.wait(&pool.mutex);
		}
	}
}

<*
 Awaits all tasks in the queue with specified priority.

 @require self.is_initialized() : "Pools must be already initialized"
*>
fn void ThreadPool.await_with_priority(&self, TaskPriority priority)
{
	if (priority == HIGH)
	{
		PoolData* pool = &self.high_pool;
		pool.mutex.lock();
		defer pool.mutex.unlock();

		while (!self.high_queue.is_empty() || self.high_threads_working.or())
		{
			pool.cond_task_finished.wait(&pool.mutex);
		}
	}
	else
	{
		PoolData* pool = &self.low_pool;
		pool.mutex.lock();
		defer pool.mutex.unlock();

		while (!self.low_queue.is_empty() || self.low_threads_working.or())
		{
			pool.cond_task_finished.wait(&pool.mutex);
		}
	}
}

<*
 Submits a new task to the pool.
*>
fn void ThreadPool.submit_task(&self, ThreadTask task, TaskPriority priority)
{
	if (priority == HIGH)
	{
		PoolData* pool = &self.high_pool;
		pool.mutex.lock();
		defer pool.mutex.unlock();

		while (true)
		{
			if (!self.high_queue.is_full())
			{
				self.high_queue.push(task);
				// signal the thread that work is available.
				pool.cond_task_acquired.broadcast();
				return;
			}
			pool.cond_task_finished.wait(&pool.mutex);
		}
	}
	else
	{
		PoolData* pool = &self.low_pool;
		pool.mutex.lock();
		defer pool.mutex.unlock();

		while (true)
		{
			if (!self.low_queue.is_full())
			{
				self.low_queue.push(task);
				// signal the thread that work is available.
				pool.cond_task_acquired.broadcast();
				return;
			}
			pool.cond_task_finished.wait(&pool.mutex);
		}
	}
}

<*
 Submits multiple tasks to the pool.
*>
fn void ThreadPool.submit_task_many(&self, ThreadTaskPack[] task_packs)
{
	PoolData* pool = null;
	sz pushed = 0;
	sz new_pushed = 0;
	sz remain = 0;

	foreach OUT: (pack : task_packs)
	{
		if (pack.priority == HIGH)
		{
			pool = &self.high_pool;
			pushed = 0;
			new_pushed = 0;
			remain = 0;

			pool.mutex.lock();
			defer pool.mutex.unlock();

			while (true)
			{
				remain = self.high_queue.buf.len - self.high_queue.len();
				new_pushed = math::min(pack.tasks.len - pushed, remain);
				self.high_queue.push_all(pack.tasks[pushed:new_pushed]);
				// signal the thread that work is available.
				pool.cond_task_acquired.broadcast();
				pushed = new_pushed;
				if (pushed == pack.tasks.len) continue OUT;
				pool.cond_task_finished.wait(&pool.mutex);
			}
		}
		else
		{
			pool = &self.low_pool;
			pushed = 0;
			new_pushed = 0;
			remain = 0;

			pool.mutex.lock();
			defer pool.mutex.unlock();

			while (true)
			{
				remain = self.low_queue.buf.len - self.low_queue.len();
				new_pushed = math::min(pack.tasks.len - pushed, remain);
				self.low_queue.push_all(pack.tasks[pushed:new_pushed]);
				// signal the thread that work is available.
				pool.cond_task_acquired.broadcast();
				pushed = new_pushed;
				if (pushed == pack.tasks.len) continue OUT;
				pool.cond_task_finished.wait(&pool.mutex);
			}
		}
	}
}

<*
 Internal procedure for running high-priority threads.
*>
fn int process_high_prio_task(void* arg) @local
{
	ThreadCreateData* thread_data = arg;
	sz thread_index = thread_data.index;
	ThreadPool* self = thread_data.tpool;
	PoolData* pool = &self.high_pool;

	mem = thread_data.mem;
	tmem = thread_data.tmem;

	@pool_init(tmem, THREAD_TEMP_MEM_AMOUNT)
	{
		while (true)
		{
			pool.mutex.lock();
			// Wait for work.
			while (self.high_queue.is_empty())
			{
				if (pool.stop)
				{
					// Shutdown requested.
					pool.alive_thread_count--;
					pool.mutex.unlock();
					return 0;
				}

				self.high_threads_working[thread_index] = false;
				pool.cond_task_acquired.wait(&pool.mutex);

				if (pool.stop_now)
				{
					// Shutdown requested.
					pool.alive_thread_count--;
					pool.mutex.unlock();
					return 0;
				}
			}
			ThreadTask task = self.high_queue.pop_first()!!;
			self.high_threads_working[thread_index] = true;
			pool.mutex.unlock();
			// Process the task.
			task.func(task.arg);
			pool.cond_task_finished.broadcast();
		}
	};
}

<*
 Internal procedure for running low-priority threads.
*>
fn int process_low_prio_task(void* arg) @local
{
	ThreadCreateData* thread_data = arg;
	sz thread_index = thread_data.index;
	ThreadPool* self = thread_data.tpool;
	PoolData* pool = &self.low_pool;

	mem = thread_data.mem;
	tmem = thread_data.tmem;

	@pool_init(tmem, THREAD_TEMP_MEM_AMOUNT)
	{
		while (true)
		{
			pool.mutex.lock();
			// Wait for work.
			while (self.low_queue.is_empty())
			{
				if (pool.stop)
				{
					// Shutdown requested.
					pool.alive_thread_count--;
					pool.mutex.unlock();
					return 0;
				}

				self.low_threads_working[thread_index] = false;
				pool.cond_task_acquired.wait(&pool.mutex);

				if (pool.stop_now)
				{
					// Shutdown requested.
					pool.alive_thread_count--;
					pool.mutex.unlock();
					return 0;
				}
			}
			ThreadTask task = self.low_queue.pop_first()!!;
			self.low_threads_working[thread_index] = true;
			pool.mutex.unlock();
			// Process the task.
			task.func(task.arg);
			pool.cond_task_finished.broadcast();
		}
	};
}


<*
 Stop all the threads and cleanup the pool.
 Any pending work will be dropped.
*>
fn void ThreadPool.destroy(&self)
{
	@shutdown(self, &self.high_pool, self.high_pool.stop_now);
	@shutdown(self, &self.low_pool, self.low_pool.stop_now);
}

<*
 Tells if subpools are initialized.
*>
fn bool ThreadPool.is_initialized(&self)
{
	return self.high_pool.initialized && self.low_pool.initialized;
}

<*
 Stop all the threads and cleanup the pool.
 Any pending work will be processed.
*>
fn void ThreadPool.stop_and_destroy(&self)
{
	@shutdown(self, &self.high_pool, self.high_pool.stop);
	@shutdown(self, &self.low_pool, self.low_pool.stop);
}

macro void @shutdown(ThreadPool* self, PoolData* pool, bool #stop) @local
{
	if (pool.initialized)
	{
		pool.mutex.lock();
		#stop = true;
		pool.cond_task_acquired.broadcast();
		pool.mutex.unlock();
		// Wait for all threads to shutdown.
		while (true)
		{
			pool.mutex.lock();
			defer pool.mutex.unlock();
			if (!self.high_threads_working.or() && !self.low_threads_working.or())
			{
				break;
			}
			pool.cond_task_acquired.signal();
		}
		pool.mutex.destroy();
		pool.cond_task_acquired.destroy();
		pool.cond_task_finished.destroy();
		pool.cond_collect.destroy();
		pool.initialized = false;
	}
}
