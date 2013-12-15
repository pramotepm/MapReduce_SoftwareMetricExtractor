package jp.naist.sdlab.utils;
import java.io.IOException;
import java.util.TreeMap;

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
		TreeMap<String, String> sorted ;
		// getArguments
		sorted = new TreeMap<String, String>(this.conf.getValByRegex("metric.args.*"));
		this.args = sorted.values().toArray(new String[sorted.values().size()]);
		
		// getMainClass
		sorted = new TreeMap<String, String>(this.conf.getValByRegex("metric.main.class.*"));
		this.mainClasses = sorted.values().toArray(new String[sorted.values().size()]);

		// getClassPath
		this.classpath = "";
		try {
			Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(this.conf);
			for (Path cacheFile : localCacheFiles) {
				if (cacheFile.toString().endsWith(".jar"))
					classpath += cacheFile.toString() + ":";
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public int getNumberOfCommand() {
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