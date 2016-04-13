package edu.umn.cs.Nebula.application;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

public class FileStreaming {

	private static final int bufferSize = 4 * 1024;

	public void sendFile(OutputStream out, String filename) {
		byte[] buffer = new byte[bufferSize];
		FileInputStream in = null;

		try {
			in = new FileInputStream(filename);

			int readByte;
			int offset = 0;
			while ((readByte = in.read(buffer, offset, bufferSize)) > -1) {
				out.write(buffer, 0, readByte);
				offset += readByte;
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
