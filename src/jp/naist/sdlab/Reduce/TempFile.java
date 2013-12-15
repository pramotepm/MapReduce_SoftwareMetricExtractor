package jp.naist.sdlab.Reduce;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

public class TempFile {
	File f;
	
	public TempFile() {
		f = new File(UUID.randomUUID().toString() + ".java");
	}

	public void write(String content) {
		try {
			FileOutputStream fos = new FileOutputStream(f, false);
			fos.write(content.getBytes());
			fos.close();
		}
		catch (IOException io) {
			io.printStackTrace();
		}
	}
	
	public String getPath() {
		return f.getAbsolutePath();
	}
	
	public void close() {
		f.delete();
	}
}