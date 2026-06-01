module ctree;

import std::io;
import std::net::tcp;
import std::thread;

const WORKERS   = 4;
const QUEUE_CAP = 1024;

struct Job
{
	tcp::TcpSocket client;
}

struct JobQueue
{
	Job[QUEUE_CAP] jobs;
	usz head;
	usz tail;
	usz count;

	thread::Mutex lock;
	thread::ConditionVariable has_jobs;
	thread::ConditionVariable has_space;

	bool shutting_down;
}

fn void queue_init(JobQueue* q)
{
	q.head = 0;
	q.tail = 0;
	q.count = 0;
	q.shutting_down = false;

	q.lock.init()!!;
	q.has_jobs.init()!!;
	q.has_space.init()!!;
}

fn void queue_push(JobQueue* q, Job job)
{
	q.lock.lock();
	defer q.lock.unlock();

	while (q.count == QUEUE_CAP && !q.shutting_down)
	{
		q.has_space.wait(&q.lock);
	}

	if (q.shutting_down)
	{
		job.client.close()!!;
		return;
	}

	q.jobs[q.tail] = job;
	q.tail = (q.tail + 1) % QUEUE_CAP;
	q.count++;

	q.has_jobs.signal();
}

fn bool queue_pop(JobQueue* q, Job* job)
{
	q.lock.lock();
	defer q.lock.unlock();

	while (q.count == 0 && !q.shutting_down)
	{
		q.has_jobs.wait(&q.lock);
	}

	if (q.shutting_down && q.count == 0)
	{
		return false;
	}

	*job = q.jobs[q.head];
	q.head = (q.head + 1) % QUEUE_CAP;
	q.count--;

	q.has_space.signal();

	return true;
}

fn void handle_client(tcp::TcpSocket client)
{
	defer client.close()!!;

	char[4096] buf;
	sz n = client.read(buf[..])!!;

	if (n > 0)
	{
		io::printfn("received %d bytes", n);

		char[] body = "OK\n";
		client.write(body)!!;
	}
}

fn int worker_main(void* ctx)
{
	JobQueue* q = (JobQueue*)ctx;
	Job job;

	while (queue_pop(q, &job))
	{
		handle_client(job.client);
	}

	return 0;
}

fn int main(String[] args)
{
	JobQueue* queue = mem::new(JobQueue);
	queue_init(queue);

	thread::Thread[WORKERS] pool;

	foreach (&worker : pool)
	{
		worker.create((ThreadFn)&worker_main, queue)!!;
	}

	tcp::TcpServerSocket listener = tcp::listen("0.0.0.0", 5000, 128)!!;
	defer listener.close()!!;

	io::printfn("Listening on port 5000")!!;

	while (true)
	{
		tcp::TcpSocket client = tcp::accept(&listener)!!;
		Job job               = { .client = client };
		queue_push(queue, job);
	}
}
