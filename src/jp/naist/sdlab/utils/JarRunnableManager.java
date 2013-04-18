package jp.naist.sdlab.utils;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

public class JarRunnableManager {
	private Configuration conf;
	private String[] args = null;
	private String[] mainClasses = null;
	private String classpath = null;
	
	public JarRunnableManager(Configuration conf) {
		this.conf = conf;
		define();
	}

	private void define() {
		// getArguments
		Collection<String> temp2 = this.conf.getValByRegex("metric.args.*").values();
		this.args = temp2.toArray(new String[temp2.size()]);
		
		// getMainClass
		Collection<String> temp1 = this.conf.getValByRegex("metric.main.class.*").values();
		this.mainClasses = temp1.toArray(new String[temp1.size()]);

		// getClassPath
		this.classpath = "";
		try {
			Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(this.conf);
			if (localCacheFiles.length > 0) {
				classpath += localCacheFiles[0].toString();
				for (int i=1; i<localCacheFiles.length; i++) {
					System.out.println(localCacheFiles[i]);
					classpath += ":" + localCacheFiles[i];
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public int getNoCommand() {
		return this.mainClasses.length;
	}
	
	public String[] getArguments() {
		return this.args;
	}

	public String[] getMainClass() {
		return this.mainClasses;
	}
	
	public String getClassPath() {
		return this.classpath;
	}
}