package ch.epfl.dlab.torcrawler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import com.google.gson.Gson;
import com.squareup.tape2.ObjectQueue.Converter;

/**
 * @author Antoine Masanet
 *
 * Converter which uses GSON to serialize instances of class T to disk. 
 * Meant to be used to write to square/tape persistent ObjectQueue
 *
 * @param <T>: The class type
 */
public final class GsonConverter<T> implements Converter<T> {
  private final Gson gson;
  private final Class<T> type;

  public GsonConverter(Gson gson, Class<T> type) {
    this.gson = gson;
    this.type = type;
  }

  @Override public T from(byte[] bytes) {
    Reader reader = new InputStreamReader(new ByteArrayInputStream(bytes));
    return gson.fromJson(reader, type);
  }

@Override public void toStream(T object, OutputStream bytes) throws IOException {
    Writer writer = new OutputStreamWriter(bytes);
    gson.toJson(object, writer);
    writer.close();
  }
}