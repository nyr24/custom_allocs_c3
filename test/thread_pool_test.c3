module thread_pool_test @test;
import data_structures::thread_pool;
import custom_allocators::arena;
import std::math;
import std::time;
import std::io;

const String[*] FILE_NAMES = {
	"resources/input0.txt",
	"resources/input1.txt",
	"resources/input2.txt",
	"resources/input3.txt",
	"resources/input4.txt",
	"resources/input5.txt",
	"resources/input6.txt",
};

fn void general() @test
{
	ThreadArena thread_alloc;
	thread_alloc.init(mem, 1ull << 32);

	const TASK_COUNT = 100;

	ThreadPool{4, 4, TASK_COUNT, TASK_COUNT} pool;
	pool.init(mem, &thread_alloc)!!;

	defer {
		pool.stop_and_destroy();
		thread_alloc.free();
	}

	ThreadTask[TASK_COUNT] compute_tasks;
	int[TASK_COUNT] primes; 
	
	foreach (int i, &p : primes)
	{
		*p = i*i*i;
	}

	foreach (i, &task : compute_tasks)
	{
		task.func = &task_computation;
		task.arg = (void*)&primes[i];
	}

	ThreadTask[TASK_COUNT] io_tasks;
	String f_name = "input.txt";
	
	foreach (i, &task : io_tasks)
	{
		task.func = &task_background;
		task.arg = (void*)&FILE_NAMES[random::rand(FILE_NAMES.len)];
	}

	ThreadTaskPack io_pack = { LOW, io_tasks[..] };
	ThreadTaskPack compute_pack = { HIGH, compute_tasks[..] };
	pool.submit_task_many((&io_pack)[:1]);
	pool.submit_task_many((&compute_pack)[:1]);
}

fn void await_test() @test
{
	ThreadArena thread_alloc;
	thread_alloc.init(mem, 1ull << 32);

	const TASK_COUNT = FILE_NAMES.len;

	ThreadPool{4, 4, TASK_COUNT, TASK_COUNT} pool;
	pool.init(mem, &thread_alloc)!!;

	defer {
		pool.stop_and_destroy();
		io::printn(thread_alloc);
		thread_alloc.free();
	}

	// First portion
	ThreadTask[TASK_COUNT / 2] io_tasks_first;
	
	foreach (i, &task : io_tasks_first)
	{
		task.func = &task_background;
		task.arg = (void*)&FILE_NAMES[i + TASK_COUNT / 2];
	}

	ThreadTaskPack io_pack = { LOW, io_tasks_first[..] };
	pool.submit_task_many((&io_pack)[:1]);
	pool.await_with_priority(LOW);
	
	// Second Portion
	ThreadTask[TASK_COUNT / 2] io_tasks_second;
	
	foreach (i, &task : io_tasks_second)
	{
		task.func = &task_background;
		task.arg = (void*)&FILE_NAMES[i];
	}

	ThreadTaskPack io_pack_sec = { LOW, io_tasks_second[..] };
	pool.submit_task_many((&io_pack_sec)[:1]);
	pool.await_with_priority(LOW);
}

module thread_pool_test;
import std::time;
import std::math;
import std::io;
import custom_allocators::arena;
import data_structures::thread_pool;
import std::atomic;

fn void task_background(void* arg)
{
	String f_name = *(String*)arg;
	ThreadArena* arena = (ThreadArena*)tmem;

	arena.@temp_scope(; alloc)
	{
		String contents = (String)file::load(alloc, f_name)!!;

		sz[] ints = alloc::alloc_array(alloc, sz, 10);
		foreach (i, &n : ints)
		{
			*n = i;
		}
		ints = alloc::realloc_array(alloc, ints, sz, 20);
		foreach (i, &n : ints[10:10])
		{
			*n = i + 10;
		}

		Atomic{sz} line_count;
		Atomic{sz} a_line_count;
		line_count.store(0);
		a_line_count.store(0);

		Splitter splitter = contents.tokenize("\n");
	
		while (try line = splitter.next())
		{
			if (line[0] == 'a')
			{
				a_line_count.add(1);
			}
			line_count.add(1);
		}
	};
}

fn void task_computation(void* arg) 
{
	int start = *(int*)arg;
	int next_p = next_prime(start);
}

fn bool is_prime(int x)
{
    if (x < 2) return false;
    for (int i = 2; i < x; ++i)
    {
        if (x % i == 0)
		{
            return false;
		}
    }
    return true;
}

fn int next_prime(int x)
{
    for (; !is_prime(x); ++x)
	{}
    return x;
}
