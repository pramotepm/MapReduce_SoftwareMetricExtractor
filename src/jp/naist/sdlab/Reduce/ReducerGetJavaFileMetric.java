package jp.naist.sdlab.Reduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import jp.naist.sdlab.utils.JarRunnableManager;
import jp.naist.sdlab.utils.ShellUnixCommandExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReducerGetJavaFileMetric extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	private Configuration conf;
	
	public String locateGitDir(Path[] distribPaths) {
		for (Path path : distribPaths) {
			if (path.getName().endsWith(".git"))
				return path.toString();
		}
		return null;
	}
	
	public List<List<String>> getExecuteCommand() {
		JarRunnableManager jM = new JarRunnableManager(this.conf);
		List<List<String>> commands = new LinkedList<List<String>>();
		for (int i=0; i<jM.getNoCommand(); i++) {
			List<String> command = new LinkedList<String>();
			command.add("java");
			command.add("-classpath");
			command.add(jM.getClassPath());
			command.add(jM.getMainClass()[i]);
			for (String arg : jM.getArguments()[i].split(","))
				command.add(arg);
			commands.add(command);
		}
		return commands;
	}
	
	@Override
	public void configure(JobConf conf) {
		this.conf = conf;
	}

	@Override
	public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		Text newVal = new Text();
		List<List<String>> commands = getExecuteCommand();
		while (value.hasNext()) {
			String record = value.next().toString();
			StringTokenizer token = new StringTokenizer(record.toString());
			String date = token.nextToken();
			String SHAKey = token.nextToken();
			String javaFile = token.nextToken();
			
			String gitDir = locateGitDir(DistributedCache.getLocalCacheFiles(this.conf));
			
			ShellUnixCommandExecutor shellRevisionFile = new ShellUnixCommandExecutor(new String[] { "git", "--git-dir", gitDir, "cat-file", "-p", SHAKey + ":" + javaFile});
			shellRevisionFile.execute();
			String javaRevisionFile = shellRevisionFile.getOutput();
						
			String xmlString = null;
			for (int i=0; i<commands.size(); i++) {
				ShellUnixCommandExecutor shellMetric = new ShellUnixCommandExecutor(commands.get(i), javaRevisionFile);
				shellMetric.execute();
				xmlString = shellMetric.getOutput();
			}		
			newVal.set(date + "\t" + JavaNCSS.extract(xmlString));
			out.collect(key, newVal);
		}
	}
}