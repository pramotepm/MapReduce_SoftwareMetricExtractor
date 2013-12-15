package jp.naist.sdlab.main;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Preprocess {
	
	public static String getTempInputFile(FileSystem fs, String repositoryDir) throws IOException {
		String inputPathInFS = fs.getConf().get("mapred.input.dir");
		Path tempInputFile =  new Path(inputPathInFS + (inputPathInFS.endsWith("/") ? "" : "/") + "index.info");
		
		if (fs.exists(tempInputFile))
			fs.delete(tempInputFile, false);
		
		FSDataOutputStream out = fs.create(tempInputFile);
		int index = 0;
		
		int fileCount = fs.getConf().getInt("preprocess.file.size", 0);
		boolean isFileLimit = fileCount != 0;
		
		Set<String> javaFiles = listJavaFiles(new File(repositoryDir));
		for (String path : javaFiles) {
			
			if (isFileLimit && fileCount-- <= 0)
				break;
			
			InputStream is = new ByteArrayInputStream((index++ + "\t" + path.replaceFirst(repositoryDir + "/", "") + "\n").getBytes());
			byte[] b = new byte[1024];
			int numBytes = 0;
			while ((numBytes = is.read(b)) > 0) {
				out.write(b, 0, numBytes);
			}
			is.close();
		}		
		out.close();
		fs.close();
		System.out.println("Map Input Records = " + javaFiles.size());
		return tempInputFile.toString();
	}
	
	private static Set<String> listJavaFiles(File projectReposSubDir) throws IOException {
		Set<String> setOfPath = new HashSet<String>();
		File[] listFile = projectReposSubDir.listFiles();
		for (File file : listFile) {
			if (file.isFile()) {
				String filename = file.toString();
				if (file.toString().endsWith(".java")) {
					setOfPath.add(filename);
				}
			}
			else {
				setOfPath.addAll(listJavaFiles(file));
			}
		}
		return setOfPath;
	}	
}