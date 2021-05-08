package ch.epfl.dlab.torcrawler;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.squareup.tape2.ObjectQueue;
import com.squareup.tape2.QueueFile;

/*Room for improvement: 
 * - implement capacity restriction
 * - find a better way to deal with I/O exceptions
 * - have one lock per subqueue
 *
 */

/*Remark: this algorithm could be abstracted to any types <E>, 
 * it would just need to have an ObjectConverter<E> and a function from <E> to
 * string to determine to which subqueue the object willl be mapped*/

/**
 * @author Antoine Masanet
 * 
 *         Round Robin, thread safe, concurrent, persistent blocking queue using
 *         square/tape persistent ObjectQueue. This class enables the storage of
 *         urls in persistent subqueues with 1 subqueue per url domain. To
 *         enforce balanced querying of url, the fetching mechanism happens in
 *         round. At the start of a round, one url is picked from each domain
 *         subqueue and stored is the urlsRound queue. When a thread fetches a
 *         url from the queue, this urls is picked at the top of the linked
 *         list. When a thread puts a url in the queue, this url is added to the
 *         corresponding subqueue according to its domain As it is a blocking
 *         queue, whenever there is no url left to pick, the fetching thread
 *         waits until another thread puts a new url in the queue
 *
 *
 *         CAVEATS: - in the java implementations, lock are lockedInterruptibly:
 *         http://fuseyism.com/classpath/doc/java/util/concurrent/LinkedBlockingQueue-source.html
 *         so that if another thread interrupts this thread, it will throw an
 *         InterruptedException. We do not deal with this particular case in
 *         this implementation. - I/O exceptions will crash the program
 * 
 *         REMARKS: the method domainDiscovered is only valid if all domains are in the domainsToQueue map 
 *         CrawlerStatistics Capacity is infinite so adding threads can always
 *         add
 */

public class RoundRobinBlockingQueue implements BlockingQueue<String>, Closeable {

	public static final String DEFAULT_FOLDER_NAME = "data/persistentRoundRobinQueue";
	public static final String SUBQUEUE_FILE_SUFFIX = ".queue";
	public static final String CURRENT_ROUND_QUEUE_NAME = "currentRound" + SUBQUEUE_FILE_SUFFIX;
	

	private Map<String, ObjectQueue<String>> domainToQueue;//Concurrent Map
	private AtomicInteger queueSize;
	private ObjectQueue<String> urlsRound;
	private final Lock lock;
	// Used to signal that the queue is not empty=> signal when add an elements
	private final Condition notEmpty;
	// Used to signal that the queue is not full (infinite capacity => not used) =>
	// signal when remove an element
	private final Condition notFull;
	private String folderName;// Name of the folder where the queue will be stored, relative to project path

	/**
	 * Create a round robin blocking queue in the specified folder. IMPORTANT:
	 * urlsRound is not initialized
	 * 
	 * @param folder: Where the subqueues should be stored
	 */
	private RoundRobinBlockingQueue(String folder) {

		assert (folder != null);

		domainToQueue = new ConcurrentHashMap<>();
		queueSize = new AtomicInteger(0);
		urlsRound = null;// IMPORTANT: the create or restore function must initialise this field
		lock = new ReentrantLock();// Lock that won't fail if acquired multiple times by same thread
		notEmpty = lock.newCondition();
		notFull = lock.newCondition();
		this.folderName = folder;
	}

	/**
	 * Create a round robin blocking queue in the default folder. IMPORTANT:
	 * urlsRound is not initialized
	 */
	private RoundRobinBlockingQueue() {
		this(DEFAULT_FOLDER_NAME);
	}

	/**
	 * Creates a new RoundRobinQueue in the following folder IMPORTANT: all queue
	 * files (.queue) and queues stored in this folder will be deleted
	 * 
	 * @param folder
	 * @return a new Round Robin blocking queue
	 */
	public static RoundRobinBlockingQueue create(String folder) {

		File dir = new File(folder);
		for (File file : dir.listFiles()) {
			if (!file.isDirectory() && file.getPath().endsWith(SUBQUEUE_FILE_SUFFIX)) {
				file.delete();
			}
		}
		RoundRobinBlockingQueue queue = new RoundRobinBlockingQueue(folder);
		queue.urlsRound = createNewQueue(folder, CURRENT_ROUND_QUEUE_NAME);

		return queue;
	}

	/**
	 * Creates a new RoundRobinQueue in the default folder IMPORTANT: all queue
	 * files (.queue) and queues stored in this folder will be deleted
	 * 
	 * @return a new Round Robin blocking queue
	 */
	public static RoundRobinBlockingQueue create() {

		return create(DEFAULT_FOLDER_NAME);
	}

	/**
	 * Restore a roundRobinBlockingQueue from the .queue files left by a previous
	 * round robin blocking queue
	 * 
	 * @param folder: the folder where the previous queue was persisted
	 * @return the restored round robin queue
	 */
	public static RoundRobinBlockingQueue restore(String folder) {
		RoundRobinBlockingQueue queue = new RoundRobinBlockingQueue(folder);

		String[] queueNames = new File(folder).list();// All the files in the folder

		queue.urlsRound = restoreQueue(folder, CURRENT_ROUND_QUEUE_NAME);
		queue.queueSize.addAndGet(queue.urlsRound.size());

		for (int i = 0; i < queueNames.length; ++i) {
			String fileName = queueNames[i];
			if (!fileName.equals(CURRENT_ROUND_QUEUE_NAME) && fileName.endsWith(SUBQUEUE_FILE_SUFFIX)) {

				ObjectQueue<String> subqueue = restoreQueue(folder, fileName);
				assert (subqueue != null);
				String domain = fileName.substring(0, fileName.lastIndexOf('.'));// Remove suffix
				queue.domainToQueue.put(domain, subqueue);
				queue.queueSize.addAndGet(subqueue.size());
			}
		}

		return queue;
	}

	/**
	 * Restore a roundRobinBlockingQueue from the .queue files left by a previous
	 * round robin blocking queue in the default folder
	 * 
	 * @return the restored round robin queue
	 */
	public static RoundRobinBlockingQueue restore() {
		return restore(DEFAULT_FOLDER_NAME);
	}

	/**
	 * Takes an input string, in this case a url and maps it to another string that
	 * will determine in which subqueue it will be stored. In this implementation,
	 * it extracts from the url its corresponding domain.
	 * 
	 * @param string
	 * @return a string indicating in which queue it will be stored
	 */
	private String stringMapper(String url) {
		
		String domain = Fetcher.getDomain(url);
		if(domain==null) {
			return "undefined";//Will place all not found domains there
		}
		return domain;
		 //return url.substring(0, 1);//for testing purposes
	}

	/**
	 * Create a subqueue with the following domain as identifier, if a queue with
	 * the same domain name already exists, it will be erased
	 * 
	 * @param domain: the identifier of the subqueue
	 * @return a new subqueue
	 */
	private ObjectQueue<String> createSubqueue(String domain) {

		String queueName = domain + SUBQUEUE_FILE_SUFFIX;
		return createNewQueue(folderName, queueName);
	}

	/**
	 * Creates a queue in the following folder with the following queueName
	 * IMPORTANT: if a queue in the same folder with the same name already exists,
	 * it will be erased
	 * 
	 * @param folderName:    where to save the queue
	 * @param queueFileName: name of the queue
	 * @return a new ObjectQueue for urls
	 */
	private static ObjectQueue<String> createNewQueue(String folderName, String queueFileName) {
		QueueFile queueFile = null;
		try {
			File file = new File(folderName, queueFileName);
			Files.deleteIfExists(file.toPath());// To make sure we always create a new file
			queueFile = new QueueFile.Builder(new File(folderName, queueFileName)).build();
		} catch (IOException e) {
			System.err.println("Could not create queueFile");
			e.printStackTrace();
			return null;
		}
		return ObjectQueue.create(queueFile, new StringConverter());
	}

	/**
	 * Restores queue from an existing queue file, returns null if file does not
	 * exist
	 * 
	 * @param folderName:    where the queue is stored
	 * @param queueFileName: the name of the file
	 * @return the restored queue
	 */
	private static ObjectQueue<String> restoreQueue(String folderName, String queueFileName) {
		QueueFile queueFile = null;
		try {
			File file = new File(folderName, queueFileName);
			if (!file.exists()) {
				System.err.println("Cannot restore queue from non existing file");
				return null;
			}
			queueFile = new QueueFile.Builder(new File(folderName, queueFileName)).build();
		} catch (IOException e) {
			System.err.println("Could not create queueFile");
			e.printStackTrace();
			return null;
		}
		return ObjectQueue.create(queueFile, new StringConverter());
	}

	/**
	 * If the current round queue is empty, refills it by taking a url from each
	 * subqueue IMPORTANT: Assumes the calling thread has the lock so no one can
	 * access the queue
	 * 
	 * @return true if a new round with at least one item has been generated
	 */
	private boolean optionallyGenerateNewRound() {
		if (!urlsRound.isEmpty()) {
			return false;
		}

		if (isEmpty()) {
			System.out.println("There are no more threads in the queue, cannot generate a new round");
			return false;
		}

		for (ObjectQueue<String> queue : domainToQueue.values()) {
			String url = null;
			try {
				url = queue.peek();
				if (url != null) {
					urlsRound.add(url);
					queue.remove();
				}
			} catch (IOException e) {
				System.err.println("Cannot peek or remove elements from a subqueue");
				e.printStackTrace();
			}
		}

		if (urlsRound.isEmpty()) {// Should never happen, if it happens, the program should crash
			throw new IllegalStateException();
		}

		return true;
	}

	@Override
	public boolean add(String url) {
		if (url == null) {
			throw new NullPointerException();
		}

		String domain = stringMapper(url);

		if (domain == null) {
			System.err.println("Could not extract domain from url using stringMapper");
			return false;
		}

		lock.lock();
		try {
			ObjectQueue<String> subqueue = domainToQueue.get(domain);

			if (subqueue == null) {// This is a new domain=> create new queue
				subqueue = createSubqueue(domain);
				domainToQueue.put(domain, subqueue);
			}

			try {
				subqueue.add(url);
				queueSize.getAndIncrement();
				notEmpty.signalAll();// Signal a consumer thread to wake him up if it is sleeping
				return true;

			} catch (IOException e) {
				System.err.println("Could not add url" + url + "to subqueue");
				e.printStackTrace();
				return false;
			}

		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean offer(String url) {
		return add(url);// There is no capacity restriction
	}

	@Override
	public void put(String url) throws InterruptedException {
		add(url);// There is no capacity restriction
	}

	@Override
	public boolean offer(String url, long timeout, TimeUnit unit) throws InterruptedException {
		return add(url);
	}

	/**
	 * Assumes that the calling thread has the lock and that the current round queue
	 * is not empty. Removes and return the head of url round if it exists or null
	 * if not+> must verify this prior to calling the function
	 * 
	 * @return the head of url round queue and decrements the size of the queue
	 */

	private String retrieveFromRoundQueue() {

		assert (!isEmpty());// Sanity check
		assert (urlsRound.size() != 0);// Sanity check

		try {
			String url = urlsRound.peek();
			urlsRound.remove();
			queueSize.getAndDecrement();

			return url;
		} catch (IOException e) {
			System.err.println("Could not peek or remove from urlRound queue");
			throw new IllegalStateException();
		}
	}

	@Override
	public String remove() {
		lock.lock();
		try {
			if (isEmpty()) {
				System.err.println("Cannot remove from an empty queue");
				throw new NoSuchElementException();
			}

			optionallyGenerateNewRound();
			return retrieveFromRoundQueue();

		} finally {
			lock.unlock();
		}
	}

	@Override
	public String poll() {
		lock.lock();
		try {
			if (isEmpty()) {
				return null;
			}

			optionallyGenerateNewRound();
			return retrieveFromRoundQueue();

		} finally {
			lock.unlock();
		}
	}

	@Override
	public String poll(long timeout, TimeUnit unit) throws InterruptedException {
		lock.lock();

		try {
			long timeoutNanos = unit.toNanos(timeout);
			while (isEmpty() && timeoutNanos > 0) {
				timeoutNanos = notEmpty.awaitNanos(timeoutNanos);
			}

			return poll();// Not a problem if using lock twice because Reentrant lock

		} finally {
			lock.unlock();
		}
	}

	@Override
	public String take() throws InterruptedException {

		lock.lock();
		try {
			while (isEmpty()) {
				try {
					notEmpty.await();// Wait for a signal emitted by an adding function and free the lock
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			optionallyGenerateNewRound();
			return retrieveFromRoundQueue();

		} finally {
			lock.unlock();
		}
	}

	/**
	 * Assumes that the queue is not empty. Return the head of url round or null if
	 * it does not exists.
	 * 
	 * @return the head of url round queue
	 */
	private String peekFromRoundQueue() {
		assert (!isEmpty());// Sanity check
		lock.lock();// No deadlock beacuse reentrant lock
		try {
			try {
				return urlsRound.peek();
			} catch (IOException e) {
				System.err.println("Could not peek or remove from urlRound queue");
				throw new IllegalStateException();
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public String element() {
		lock.lock();
		try {
			if (isEmpty()) {
				throw new NoSuchElementException();
			}

			optionallyGenerateNewRound();
			return peekFromRoundQueue();

		} finally {
			lock.unlock();
		}
	}

	@Override
	public String peek() {
		lock.lock();
		try {
			if (isEmpty()) {
				return null;
			}

			optionallyGenerateNewRound();
			return peekFromRoundQueue();

		} finally {
			lock.unlock();
		}
	}

	@Override
	public int size() {
		return queueSize.get();
	}

	/**
	 * Returns the total amount of subqueues in this queue, empty subqueues are also
	 * counted Notes: this count should correspond to the number of different
	 * domains discovered
	 * 
	 * @return the total number of subqueues in the queue
	 */
	public int subqueueCount() {
		return domainToQueue.size();
	}
	
	/**
	 * Returns the fraction of subqueues that are not empty at the time of the call
	 * @return the fraction of subqueues that are not empty
	 */
	public double subqueueUtilization() {
		
			if(domainToQueue.size()==0) {
				return 1;
			}
			
			int nonEmptyCount = 0;
			for(ObjectQueue<String> subqueue: domainToQueue.values()) {
				if(!subqueue.isEmpty()) {
					nonEmptyCount++;
				}
			}

			return ((double)nonEmptyCount)/domainToQueue.size();
	}

	@Override
	public boolean isEmpty() {
		return queueSize.get() == 0;
	}

	/**
	 * Unsupported operation
	 */
	@Override
	public Iterator<String> iterator() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Return a list containing at most size elements of the queue
	 * 
	 * @param queue
	 * @param size
	 * @return a list containing at most size elements of the queue
	 */
	private List<String> queueToList(ObjectQueue<String> queue, int size) {
		assert (size >= 0);
		lock.lock();
		try {
			try {
				return queue.peek(size);
			} catch (IOException e) {
				System.err.println("Cannot peek from a subqueue");
				throw new IllegalStateException();
			}
		} finally {
			lock.unlock();
		}

	}

	/**
	 * Returns a list containing all the elements of the queue
	 * 
	 * @param queue
	 * @return a list containing all the elements of the queue
	 */
	private List<String> queueToList(ObjectQueue<String> queue) {
		return queueToList(queue, queue.size()); // No need for lock as it calls a method using locks
	}

	/**
	 * Returns a list containing all the persistent queues used for implementing
	 * this queue: - all the domain subqueues - the urlRound queue
	 * 
	 * @return a list of persistent queues of this class
	 */
	private List<ObjectQueue<String>> allQueues() {
		List<ObjectQueue<String>> allQueues = new ArrayList();
		allQueues.add(urlsRound);
		allQueues.addAll(domainToQueue.values());
		return allQueues;
	}

	@Override
	public Object[] toArray() {
		lock.lock();

		try {
			int size = queueSize.get();
			Object[] array = new Object[size];
			int i = 0;

			for (ObjectQueue<String> queue : allQueues()) {

				List<String> urls = queueToList(queue);
				for (String url : urls) {
					array[i] = url;
					i++;
				}
			}

		} finally {
			lock.unlock();
		}

		return null;
	}

	// Inspired from:
	// http://fuseyism.com/classpath/doc/java/util/concurrent/LinkedBlockingQueue-source.html
	@Override
	public <T> T[] toArray(T[] array) {
		lock.lock();

		try {
			int size = queueSize.get();
			if (array.length < size) {
				array = (T[]) java.lang.reflect.Array.newInstance(array.getClass().getComponentType(), size);
			}

			int i = 0;

			for (ObjectQueue<String> queue : allQueues()) {

				List<String> urls = queueToList(queue);
				for (String url : urls) {
					array[i] = (T) url;
					i++;
				}
			}
			if (array.length > i) {
				array[i] = null;
			}

		} finally {
			lock.unlock();
		}
		return null;
	}

	@Override
	public boolean addAll(Collection<? extends String> collection) {
		lock.lock();
		try {
			if (collection == null) {
				throw new NullPointerException();
			}

			if (collection.isEmpty()) {
				return false;
			}

			boolean changed = false;
			for (String s : collection) {
				if (add(s)) {
					changed = true;
				}
			}
			notEmpty.signalAll();
			return changed;
		} finally {
			lock.unlock();
		}

	}

	/**
	 * Unsupported Operation
	 */
	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Unsupported Operation
	 */
	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();// Can only remove head=> not possible
	}

	/**
	 * Unsupported Operation
	 */
	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();// Can only remove head=> not possible
	}

	@Override
	public void clear() {
		lock.lock();
		try {
			try {
				for (ObjectQueue<String> queue : allQueues()) {
					queue.clear();
				}

			} catch (IOException e) {
				System.err.println("Could not clear a subqueue");
				throw new IllegalStateException();
			}

			queueSize.set(0);
		} finally {
			lock.unlock();
		}

	}

	@Override
	public void close() throws IOException {
		lock.lock();
		try {

			for (ObjectQueue<String> queue : allQueues()) {
				queue.close();
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int remainingCapacity() {
		return Integer.MAX_VALUE;// There is no limit to capacity
	}

	/**
	 * Can only remove the object if it is the head of the current round queue or of
	 * one of the underlying domain subqueues
	 */
	@Override
	public boolean remove(Object o) {
		if (o == null) {
			return false;
		}

		lock.lock();
		try {
			try {
				for (ObjectQueue<String> queue : allQueues()) {

					if (o.equals(queue.peek())) {
						queue.remove();
						return true;
					}
				}
			} catch (IOException e) {
				System.err.println("Could not peek or remove from queue");
				throw new IllegalStateException();
			}

			return false;

		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean contains(Object o) {
		if (o == null) {
			return false;
		}
		lock.lock();

		try {

			for (ObjectQueue<String> queue : allQueues()) {
				List<String> urls = queueToList(queue);
				if (urls.contains(o)) {
					return true;
				}
			}
			return false;

		} finally {
			lock.unlock();
		}
	}

	@Override
	public int drainTo(Collection<? super String> collection) {
		return drainTo(collection, queueSize.get());
	}

	@Override
	public int drainTo(Collection<? super String> collection, int maxElements) {
		if (collection == null) {
			throw new NullPointerException();
		}

		if (collection == this) {
			throw new IllegalArgumentException();
		}

		lock.lock();

		int remainingSpace = maxElements;
		try {

			for (ObjectQueue<String> queue : allQueues()) {
				try {
					int peekSize = Math.min(remainingSpace, queue.size());
					collection.addAll(queue.peek(peekSize));
					remainingSpace -= peekSize;
					if (remainingSpace == 0) {
						return maxElements;
					}

				} catch (IOException e) {
					System.err.println("Could not peek or remove from queue");
					e.printStackTrace();
				}
			}

			return maxElements - remainingSpace;

		} finally {
			lock.unlock();
		}

	}

	/**
	 * Adds all the elements of the queue in a list
	 * @return a list of all the urls in the queue
	 * @throws IOException
	 */
	public List<String> toList() throws IOException {
		List<String> list = new ArrayList<>();

		for (ObjectQueue<String> queue : allQueues()) {
			list.addAll(queueToList(queue));
		}

		return list;
	}

	/**
	 * Adds all the elements of the queue in a set
	 * @return a set of all the urls in the queue
	 * @throws IOException
	 */
	public Set<String> toSet() {
		Set<String> set = new HashSet<>();

		for (ObjectQueue<String> queue : allQueues()) {
			set.addAll(queueToList(queue));
		}

		return set;
	}
	
	
	/**
	 * Returns the number of domains discovered since the create of the queue
	 * @return the number of domains discovered
	 */
	public int domainsDiscovered() {
		return domainToQueue.size();
	}

	@Override
	public String toString() {
		lock.lock();
		try {
			StringBuilder sb = new StringBuilder();

			sb.append("Queue size:" + queueSize + "\n");

			sb.append("CurrentRoundQueue:" + queueToList(urlsRound) + "\n");

			for (Map.Entry<String, ObjectQueue<String>> entry : domainToQueue.entrySet()) {

				ObjectQueue<String> subqueue = entry.getValue();
				List<String> subqueueList = queueToList(subqueue);

				sb.append("Subqueue " + entry.getKey() + ":" + subqueueList + "\n");

			}
			return sb.toString();
		} finally {
			lock.unlock();
		}
	}
}
