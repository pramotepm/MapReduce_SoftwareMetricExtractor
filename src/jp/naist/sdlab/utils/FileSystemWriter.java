package jp.naist.sdlab.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemWriter {

	public static void WriteAll(Path p, Configuration conf, String s) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		FSDataOutputStream out = fileSystem.create(p);
		InputStream is = new ByteArrayInputStream(s.getBytes());
		
		byte[] b = new byte[1024];
		int numBytes = 0;
		while ((numBytes = is.read(b)) > 0) {
			out.write(b, 0, numBytes);
		}

		is.close();
		out.close();
		fileSystem.close();
	}
}
