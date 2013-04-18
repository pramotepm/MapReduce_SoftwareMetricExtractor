package jp.naist.sdlab.Map;

import java.io.IOException;
import jp.naist.sdlab.utils.ShellUnixCommandExecutor;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapperGetSHAKeyJavaFile extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	private JobConf conf;
	
	public String locateGitDir(Path[] distribPaths) {
		for (Path path : distribPaths) {
			if (path.getName().endsWith(".git"))
				return path.toString();
		}
		return null;
	}
	
	@Override
	public void configure(JobConf conf) {
		this.conf = conf;
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		String[] tmp = value.toString().split("\t");
		String id = tmp[0];
		String javaFile = tmp[1];
		
		String gitDir = locateGitDir(DistributedCache.getLocalCacheFiles(conf));
		
		// --git-dir value, I will change later by reference from variable that can get from configuration (conf.get())
		ShellUnixCommandExecutor shell = new ShellUnixCommandExecutor(new String[] { "git", "--git-dir", gitDir, "log", "--format=\"%ad %H\"", "--date=short", "--", javaFile });
		shell.execute();
		String outputs = shell.getOutput();
				
		String date = null;
		String SHAKey = null;
		Text tID = new Text(id);
		Text tVal = new Text();
		for (String record : outputs.split(System.getProperty("line.separator"))) {
			String[] field = record.replaceAll("\"", "").split("\\s");
			date = field[0];
			SHAKey = field[1];
			tVal.set(date + "\t" + SHAKey + "\t" + javaFile);
			out.collect(tID, tVal);
		}
	}
}