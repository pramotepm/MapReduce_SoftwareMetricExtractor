package jp.naist.sdlab.utils;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class FileSystemReader {
	public static String readAll(Path filepath, Configuration conf) {
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(conf);
			if (!fileSystem.exists(filepath)) {
				System.out.println("File does not exists");
				return null;
			}
		} catch (IOException e) {
			System.err.println("IOException cause FileSystem");
		}
		FSDataInputStream in = null;
		try {
			in = fileSystem.open(filepath);
		} catch (IOException e) {
			System.err.println("IOException cause FSDataInputStream");
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputStream out = new BufferedOutputStream(baos);
		byte[] b = new byte[1024];
		int numBytes = 0;
		try {
			while ((numBytes = in.read(b)) > 0) {
				out.write(b, 0, numBytes);
			}
		} catch (IOException e) {
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				fileSystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				baos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
		String output = null;
		try {
			output = new String(baos.toByteArray(), "utf-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return output;
	}
}