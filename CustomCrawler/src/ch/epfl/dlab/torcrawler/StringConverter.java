package ch.epfl.dlab.torcrawler;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import com.squareup.tape2.ObjectQueue.Converter;

/**
 * @author Antoine Masanet
 *
 * Converter that converts String<->bytes[]. 
 * Meant to be used to write strings to square/tape persistent ObjectQueue
 */
public final class StringConverter implements Converter<String>{

	@Override
	public String from(byte[] bytes) throws IOException {
		return new String(bytes, StandardCharsets.UTF_8);
	}

	@Override
	public void toStream(String string, OutputStream outputStream) throws IOException {
		outputStream.write(string.getBytes(StandardCharsets.UTF_8));
	}
}
