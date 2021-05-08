package ch.epfl.dlab.torcrawler;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.squareup.tape2.ObjectQueue;
import com.squareup.tape2.ObjectQueue.Converter;
import com.squareup.tape2.QueueFile;

//NOTE: not used in the most recent implementation

/*TODO: have create destroy the previous queue and add a restore function to restore a previous queue
		look at RoundRobinBlockingQueue for inspiration
*/

/**
 * @author Antoine Masanet
 *
 *         FIFO thread safe concurrent persistent queue using square/tape
 *         persistent ObjectQueue
 * 
 *         CAVEATS: - A queue with no capacity provided will
 *         still have a max capacity of Integer.MAX_VALUE
 *         -I/O exception are thrown as unchecked exception to satisfy the blocking queue specifications
 *
 * @param <E>
 */
public class BlockingObjectQueue<E> implements BlockingQueue<E>, Closeable {

	private final ObjectQueue<E> queue;

	private final int capacity;
	private final Lock lock = new ReentrantLock();// Lock that won't fail if acquired multiple times by same thread
	private final Condition notEmpty = lock.newCondition(); // Used to signal that the queue is not empty
	private final Condition notFull = lock.newCondition(); // Used to signal that the queue is not full

	private BlockingObjectQueue(ObjectQueue<E> queue, int capacity) {
		if (capacity < 0 || queue == null) {
			throw new IllegalArgumentException();
		}

		this.queue = queue;
		this.capacity = capacity;
	}

	/**
	 * The capacity of this queue is set to Integer.MAX_VALUE
	 * 
	 * @param queue
	 */
	private BlockingObjectQueue(ObjectQueue<E> queue) {
		this(queue, Integer.MAX_VALUE);
	}

	public static <T> BlockingObjectQueue<T> create(QueueFile queueFile, Converter<T> converter) {
		if (converter == null || queueFile == null) {
			throw new IllegalArgumentException();
		}
		return new BlockingObjectQueue<>(ObjectQueue.create(queueFile, converter));
	}

	public static <T> BlockingObjectQueue<T> create(QueueFile queueFile, Converter<T> converter, int capacity) {
		if (converter == null || queueFile == null) {
			throw new IllegalArgumentException();
		}

		return new BlockingObjectQueue<>(ObjectQueue.create(queueFile, converter), capacity);
	}

	/**
	 * Whether the queue has reached its capacity limit
	 * 
	 * @return Whether the queue is full
	 */
	public boolean isFull() {
		return queue.size() >= capacity;
	}

	@Override
	public boolean add(E element) {
		lock.lock();
		try {
			if (isFull()) {
				throw new IllegalStateException();
			}
			queue.add(element);
			notEmpty.signal();// Signal a consumer thread to wake him up if it is sleeping
			return true;
		} catch (IOException e) {
			getSneakyThrowable(e);// Used to throw checked exception without having to specify it
		} finally {
			lock.unlock();
		}
		return false;
	}

	@Override
	public boolean offer(E element) {
		lock.lock();
		try {
			if (isFull()) {
				return false;
			}
			queue.add(element);
			notEmpty.signal();// Signal a consumer thread to wake him up if it is sleeping
			return true;
		} catch (IOException e) {
			getSneakyThrowable(e);// Used to throw checked exception without having to specify it
		} finally {
			lock.unlock();
		}
		return false;
	}

	@Override
	public void put(E element) throws InterruptedException {
		lock.lock();
		try {
			while (isFull()) {
				notFull.await();// Wait for a signal emitted by an adding function
			}
			queue.add(element);
			notEmpty.signal();// Signal a consumer thread to wake him up if it is sleeping
		} catch (IOException e) {
			throw getSneakyThrowable(e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public E take() throws InterruptedException {
		lock.lock();
		try {
			while (queue.isEmpty()) {
				notEmpty.await();// Wait for a signal emitted by an adding function
			}
			E peek = queue.peek();
			queue.remove();
			notFull.signal();
			return peek;
		} catch (IOException e) {
			throw getSneakyThrowable(e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public E remove() {
		lock.lock();
		try {
			if (queue.isEmpty()) {
				throw new NoSuchElementException();
			}
			E element = queue.peek();
			queue.remove();
			notFull.signal();// Signal a producer thread to wake him up if it is sleeping
			return element;
		} catch (IOException e) {
			throw getSneakyThrowable(e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public E poll() {
		lock.lock();
		try {
			if (queue.isEmpty()) {
				return null;
			}
			E element = queue.peek();
			queue.remove();
			notFull.signal();
			return element;
		} catch (IOException e) {
			throw getSneakyThrowable(e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public E element() {
		lock.lock();
		try {
			if (queue.isEmpty()) {
				throw new NoSuchElementException();
			}
			return queue.peek();
		} catch (IOException e) {
			throw getSneakyThrowable(e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public E peek() {
		lock.lock();
		try {
			if (queue.isEmpty()) {
				return null;
			}
			return queue.peek();
		} catch (IOException e) {
			throw getSneakyThrowable(e);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Reads up to max entries from the head of the queue without removing the
	 * entries. If the queue's size() is less than max then only size() entries are
	 * read
	 * 
	 * @param max
	 * @return
	 */
	public List<E> peek(int max) {
		lock.lock();
		try {
			if (queue.isEmpty()) {
				return null;
			}
			return queue.peek(max);
		} catch (IOException e) {
			throw getSneakyThrowable(e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int size() {
		lock.lock();
		try {
			return queue.size();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean isEmpty() {
		lock.lock();
		try {
			return queue.isEmpty();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Iterator<E> iterator() {
		return queue.iterator();
	}

	@Override
	public void clear() {
		lock.lock();
		try {
			queue.clear();
		} catch (IOException e) {
			throw getSneakyThrowable(e);
		} finally {
			lock.unlock();
		}

	}

	@Override
	public void close() throws IOException {
		lock.lock();
		try {
			queue.close();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int remainingCapacity() {
		lock.lock();
		try {
			return capacity - queue.size();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Object[] toArray() {
		lock.lock();
		try {
			Object[] out = new Object[queue.size()];
			int i = 0;
			for (E e : queue) {
				out[i++] = e;
			}
			return out;
		} finally {
			lock.unlock();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T[] toArray(T[] a) {
		return (T[]) toArray(); //See RoundRobinBlockingQueue.java for a better implementation
	}

	/**
	 * REMARK: as the queue is FIFO, only the head can be removed
	 */
	@Override
	public boolean remove(Object o) {
		
		if(o==null) {
			return false;
		}
		
		lock.lock();
		
		try {
			try {
				E peek = queue.peek();
				if(peek.equals(o)) {
					queue.remove();
					return true;
				}
			} catch (IOException e) {
				throw getSneakyThrowable(e);
			}
		} finally {
			lock.unlock();
		}
		return false;
	}

	@Override
	public boolean contains(Object o) {
		lock.lock();
		try {
			for (E entry : queue) {
				if (Objects.deepEquals(entry, o)) {
					return true;
				}
			}
			return false;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		lock.lock();
		try {
			for (Object e : c) {
				if (!contains(e)) {// Not problem with the multiple locks because ReentrantLock
					return false;
				}
			}
			return true;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean offer(E element, long timeout, TimeUnit unit) throws InterruptedException {
		lock.lock();
		try {
			long timeoutNanos = unit.toNanos(timeout);
			while (isFull() && timeoutNanos > 0) {
				
				// Wakes up when either a signal is emited or there is a nanos timeout
				timeoutNanos = notEmpty.awaitNanos(timeoutNanos);
			}
			return offer(element);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		lock.lock();
		try {
			long timeoutNanos = unit.toNanos(timeout);
			while (queue.isEmpty() && timeoutNanos > 0) {
				timeoutNanos = notEmpty.awaitNanos(timeoutNanos);
			}
			E peek = queue.peek();
			if (peek == null) {
				return null;
			}
			queue.remove();
			notFull.signal();
			return peek;
		} catch (IOException e) {
			throw getSneakyThrowable(e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int drainTo(Collection<? super E> collection) {
		return drainTo(collection, size());
	}

	@Override
	public int drainTo(Collection<? super E> collection, int maxElements) {

		if (collection == null)
			throw new NullPointerException();
		if (this == collection) {
			throw new IllegalArgumentException();
		}

		lock.lock();
		try {
			Iterator<E> it = queue.iterator();
			int i = 0;
			while (it.hasNext() && i < maxElements) {
				collection.add(it.next());
				it.remove();
				i++;
			}
			notFull.signalAll();
			return i;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		if (c.isEmpty()) {
			return false;
		}
		lock.lock();
		try {
			for (E e : c) {
				queue.add(e);
			}
			notEmpty.signalAll();// Signal all consumer thread to wake them up if they are waiting on this signal
			return true;
		} catch (IOException e) {
			throw getSneakyThrowable(e);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Not implemented because the underlying queue only permits the removal of the head
	 */
	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Not implemented because the underlying queue only permits the removal of the head
	 */
	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Used to throw checked exception without having to specify it: taken from
	 * tape2 source code
	 * 
	 * @param <T>
	 * @param t
	 * @return
	 * @throws T
	 */
	@SuppressWarnings({ "unchecked", "TypeParameterUnusedInFormals" })
	private static <T extends Throwable> T getSneakyThrowable(Throwable t) throws T {
		throw (T) t;
	}

}
