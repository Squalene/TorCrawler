package ch.epfl.dlab.torcrawler.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import ch.epfl.dlab.torcrawler.RoundRobinBlockingQueue;
import ch.epfl.dlab.torcrawler.PersistenceThread;

/**
 * IMPORTANT: for the tests to pass, one must change the stringMapper of RoundRobinBlockingQueue
 *
 */
public final class RoundRobinBlockingQueueTest {

	private static class ProducerThread extends Thread {
		public final int id;
		public final RoundRobinBlockingQueue queue;

		public ProducerThread(int id, RoundRobinBlockingQueue queue) {
			this.id = id;
			this.queue = queue;
		}

		@Override
		public void run() {
			int i = 0;
			while (true) {
				
				try {
					queue.put(String.valueOf(i));
				} catch (InterruptedException e1) {
					System.err.println("Cannot put in queue");
					e1.printStackTrace();
				}
				System.out.println("Producer Thread:"+id+" added element:" + i);
				++i;
				try {
					Thread.sleep(2_000);//Similar to not sleeping
				} catch (InterruptedException e) {
					System.err.println("Sleep interrupt");
					e.printStackTrace();
				}
			}

		}
	}
	
	private static class ConsumerThread extends Thread {
		public final int id;
		public final RoundRobinBlockingQueue queue;

		public ConsumerThread(int id, RoundRobinBlockingQueue queue) {
			this.id = id;
			this.queue = queue;
		}

		@Override
		public void run() {
	
			while (true) {
				try {
					String s = queue.take();
					System.out.println("Consumer Thread:"+id+" consumed element:" + s);

				} catch (InterruptedException e1) {
					System.err.println("Error while taking");
					e1.printStackTrace();
				}
			}
		}
	}
	
	

	public static void queueCanBeCreatedAndClosed() {

		RoundRobinBlockingQueue queue = RoundRobinBlockingQueue.create();

		try {
			queue.close();
		} catch (IOException e) {
			System.err.println("Cannot close queue");
			e.printStackTrace();
		}
	}

	public static void createdQueueHasNoElements() {

		RoundRobinBlockingQueue queue = RoundRobinBlockingQueue.create();

		try {
			System.out.println("New queue size is: " + queue.size());
		} finally {
			try {
				queue.close();
			} catch (IOException e) {
				System.err.println("Cannot close queue");
				e.printStackTrace();
			}
		}
	}

	public static void queueSizeIncreasesWhenAddingElements() {
		RoundRobinBlockingQueue queue = RoundRobinBlockingQueue.create();

		try {
			queue.clear();
			for (int i = 0; i < 10; ++i) {
				queue.add("a");
			}

			System.out.println("Queue size is:" + queue.size() + " and should be 10");

		} finally {
			try {
				queue.close();
			} catch (IOException e) {
				System.err.println("Cannot close queue");
				e.printStackTrace();
			}
		}
	}

	public static void addingDifferentDomainElementsCreateDifferentSubqueues() {
		RoundRobinBlockingQueue queue = RoundRobinBlockingQueue.create();

		try {
			queue.add("a");
			queue.add("b");
			queue.add("c");
			queue.add("ab");
			queue.add("cde");

			System.out.println("Queue after adding elements:" + queue);
		} finally {
			try {
				queue.close();
			} catch (IOException e) {
				System.err.println("Cannot close queue");
				e.printStackTrace();
			}
		}
	}

	public static void restoredQueueIsIdenticalToPreviouslyClosedQueue() {
		RoundRobinBlockingQueue queue = RoundRobinBlockingQueue.create();
		System.out.println("Created queue:" + queue);

		queue.add("a");
		queue.add("b");
		queue.add("c");
		queue.add("ab");
		queue.add("cde");

		System.out.println("Queue after adding elements:" + queue);

		try {
			queue.close();
		} catch (IOException e) {
			System.err.println("Cannot close created queue");
			e.printStackTrace();
		}

		RoundRobinBlockingQueue restoredQueue = RoundRobinBlockingQueue.restore();
		System.out.println("Restored queue:" + restoredQueue);

		try {
			restoredQueue.close();
		} catch (IOException e) {
			System.err.println("Cannot close restored queue");
			e.printStackTrace();
		}
	}

	public static void removesProperlyAndThrowsExceptionIfQueueIsEmpty() {
		RoundRobinBlockingQueue queue = RoundRobinBlockingQueue.create();

		try {
			queue.add("a");
			queue.add("b");
			queue.add("c");
			queue.add("ab");
			queue.add("cde");

			System.out.println("Queue after adding elements:" + queue);

			while (true) {
				System.out.println("Removed elements:" + queue.remove());
				System.out.println(queue);
			}

		} finally {
			try {
				queue.close();
			} catch (IOException e) {
				System.err.println("Cannot close queue");
				e.printStackTrace();
			}
		}
	}

	public static void takesProperlyAndWaitsIndefinitelyIfQueueIsEmpty() {
		RoundRobinBlockingQueue queue = RoundRobinBlockingQueue.create();

		try {
			queue.add("a");
			queue.add("b");
			queue.add("c");
			queue.add("ab");
			queue.add("cde");

			System.out.println("Queue after adding elements:" + queue);

			while (true) {
				try {
					System.out.println("Removed elements:" + queue.take());
				} catch (InterruptedException e) {
					System.err.println("Cannot take from queue");
					e.printStackTrace();
				}
				System.out.println(queue);
			}

		} finally {
			try {
				queue.close();
			} catch (IOException e) {
				System.err.println("Cannot close queue");
				e.printStackTrace();
			}
		}
	}

	public static void mutltiThreadedOneProducerTenConsumer() {

		RoundRobinBlockingQueue queue = RoundRobinBlockingQueue.create();

		try {

			ProducerThread producer = new ProducerThread(0,queue);
			
			
			List<ConsumerThread> consumers = new ArrayList<>();
			Set<String> discoveredUrls = new HashSet<>();

			for (int i = 1; i < 11; ++i) {
				consumers.add(new ConsumerThread(i,queue));
			}

			producer.start();
			for(ConsumerThread consumer:consumers) {
				consumer.start();
			}
			
			new PersistenceThread(discoveredUrls).start();
			
			try {
				Thread.sleep(120_000);//Run test for 2 min
			} catch (InterruptedException e) {
				System.err.println("Sleep interruption");
				e.printStackTrace();
			}

		} finally {
			try {
				System.out.println("Closing queue");
				queue.close();
			} catch (IOException e) {
				System.err.println("Cannot close queue");
				e.printStackTrace();
			}
		}
	}

	public static void addToQueueAddsElementAndCreatesSubqueue() {
		RoundRobinBlockingQueue queue = RoundRobinBlockingQueue.create();
		queue.add("a");
		try {
			System.out.println(queue.toList());
		} catch (IOException e) {
			System.err.println("Cannot convert queue to list");
			e.printStackTrace();
		}

		try {
			queue.close();
		} catch (IOException e) {
			System.err.println("Cannot close queue");
			e.printStackTrace();
		}
	}

}
