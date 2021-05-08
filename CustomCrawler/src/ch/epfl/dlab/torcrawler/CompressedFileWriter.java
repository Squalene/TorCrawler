package ch.epfl.dlab.torcrawler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;
import com.google.gson.Gson;

public class CompressedFileWriter {
	 public static long MAX_FILE_LENGTH = 250000000L;
	  
	  static Gson gson = new Gson();
	  private File currentFile = null;
	  private Writer writer = null;
	  private FileOutputStream output = null;
	  private String folder = null;
	  
	  public CompressedFileWriter(String folder) throws IOException {
	    this.folder = folder;
	    createWriter();
	    Runtime.getRuntime().addShutdownHook(new Thread() {
	          public void run() {
	            try {
	              CompressedFileWriter.this.close();
	            } catch (IOException e) {
	              e.printStackTrace();
	            } 
	          }
	        });
	  }
	  
	  private void createWriter() throws IOException {
	    String fileName = String.valueOf(UUID.randomUUID().toString()) + ".json.gz";
	    this.currentFile = new File(this.folder, fileName);
	    this.currentFile.getParentFile().mkdirs();
	    this.output = new FileOutputStream(this.currentFile);
	    this.writer = new OutputStreamWriter(new GZIPOutputStream(this.output), "UTF-8");
	  }
	  
	  public synchronized void save(Object obj) throws UnsupportedEncodingException, IOException {
	    if (this.currentFile.length() > MAX_FILE_LENGTH) {
	      close();
	      createWriter();
	    } 
	    this.writer.write(String.valueOf(gson.toJson(obj)) + "\n");
	    this.writer.flush();
	  }
	  
	  public void close() throws IOException {
	    this.writer.flush();
	    this.writer.close();
	  }
}
