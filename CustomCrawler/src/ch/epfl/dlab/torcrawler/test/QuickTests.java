package ch.epfl.dlab.torcrawler.test;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.BlockingQueue;

import com.google.gson.Gson;
import com.squareup.tape2.ObjectQueue;
import com.squareup.tape2.QueueFile;

import ch.epfl.dlab.torcrawler.BlockingObjectQueue;
import ch.epfl.dlab.torcrawler.StringConverter;

public class QuickTests {

	public static void testGson() {

		String url = "http://hssza6r6fbui4x452ayv3dkeynvjlkzllezxf3aizxppmcfmz2mg7uad.onion/";

		Gson gson = new Gson();
		String gsonUrl = gson.toJson(url);
		System.out.println("To json is:" + gsonUrl);
	}

	public static void testPersistentQueue() {
		// File file = new File("data/persistentQueue","test.queue");

		QueueFile queueFile = null;
		try {
			queueFile = new QueueFile.Builder(new File("data/persistentQueue", "test.queue")).build();
		} catch (IOException e) {
			System.err.println("Could not create queueFile");
			e.printStackTrace();
		}

		ObjectQueue<String> queue = ObjectQueue.create(queueFile, new StringConverter());

		try {
			queue.add("Hello");
			queue.add("there");

			synchronized (queue) {
			}

			System.out.println(queue.peek(Integer.MAX_VALUE));

			queue.clear();

		} catch (IOException e) {
			System.err.println("Error while peeking in queue");
			e.printStackTrace();
		}
	}

	public static void testQueueReuse() {
		// BlockingObjectQueue<String> blockingQueue = (BlockingObjectQueue<String>)
		// createNewPersistentQueue();
		BlockingObjectQueue<String> blockingQueue = (BlockingObjectQueue<String>) openPersistentQueue(
				"urlToFetch_11-29-21-7.queue");

		System.out.println(blockingQueue.peek(Integer.MAX_VALUE));

		blockingQueue.remove();
		blockingQueue.add("World");
	}

	public static void testBlockingQueue() {

		BlockingObjectQueue<String> blockingQueue = (BlockingObjectQueue<String>) createNewPersistentQueue();
		// BlockingObjectQueue<String> blockingQueue = (BlockingObjectQueue<String>)
		// openPersistentQueue("urlToFetch_11-29-21-5.queue");

		System.out.println(blockingQueue.peek(Integer.MAX_VALUE));
		blockingQueue.add("Hello");
		System.out.println(blockingQueue.peek(Integer.MAX_VALUE));
		blockingQueue.add("There");
		blockingQueue.add("General");
		System.out.println(blockingQueue.peek(Integer.MAX_VALUE));

		blockingQueue.remove();
		System.out.println(blockingQueue.peek(Integer.MAX_VALUE));
		blockingQueue.remove();
		try {
			blockingQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println(blockingQueue.peek(Integer.MAX_VALUE));

		Thread t = new Thread() {
			public void run() {
				try {
					System.out.println("Temp Thread is going to take");
					blockingQueue.take();
					System.out.println("Temp Thread took");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		t.start();

		blockingQueue.add("New data");

	}

	// BOTH FOLLOWING METHOD WERE USED WITH THE PREVIOUS SIMPLE BLOCKING QUEUE
	// ALGORITHM=> deprecated
	public static BlockingQueue<String> createNewPersistentQueue() {

		QueueFile queueFile = null;
		LocalDateTime date = LocalDateTime.now();
		String queueName = "urlToFetch_" + date.getMonthValue() + "-" + date.getDayOfMonth() + "-" + date.getHour()
				+ "-" + date.getMinute() + ".queue";
		try {
			queueFile = new QueueFile.Builder(new File("data/persistentQueue", queueName)).build();
		} catch (IOException e) {
			System.err.println("Could not create queueFile");
			e.printStackTrace();
			return null;
		}
		return BlockingObjectQueue.create(queueFile, new StringConverter());
	}

	private static BlockingQueue<String> openPersistentQueue(String queueName) {
		QueueFile queueFile = null;
		try {
			queueFile = new QueueFile.Builder(new File("data/persistentQueue", queueName)).build();
		} catch (IOException e) {
			System.err.println("Could not create queueFile");
			e.printStackTrace();
		}
		return BlockingObjectQueue.create(queueFile, new StringConverter());
	}

}
