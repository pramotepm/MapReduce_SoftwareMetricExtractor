package jp.naist.sdlab.Reduce;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import jp.naist.sdlab.utils.JarRunnableManager;
import jp.naist.sdlab.utils.SeaShell;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

public class ReducerGetJavaFileMetric extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	enum Counters { 
		NUMBER_OF_RECORDS,
		EXIT_0
	}
	
	private Configuration conf;
	
	public List<List<String>> getExecuteCommand() {
		JarRunnableManager jM = new JarRunnableManager(conf);
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
	public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> out, Reporter reporter) {
		Text newVal = new Text();
		String gitDir = new File(".git").getAbsolutePath();
		List<List<String>> commands = getExecuteCommand();
		String xmlString = null;
		
		while (value.hasNext()) {
			String record = value.next().toString();
			
			StringTokenizer token = new StringTokenizer(record.toString());
			String date = token.nextToken();
			String SHAKey = token.nextToken();
			String javaFile = token.nextToken();
			
			String[] command = new String[] { "git", "--git-dir=" + gitDir, "cat-file", "-p", SHAKey + ":" + javaFile};  
			
			ShellCommandExecutor shellRevisionFile = new ShellCommandExecutor(command);
			try {
				shellRevisionFile.execute();
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (shellRevisionFile.getExitCode() == 0) {
				String javaSourceCodeAtRevision = shellRevisionFile.getOutput();
				for (int i=0; i<commands.size(); i++) {
					SeaShell.ShellCommandExecutor shellMetric = new SeaShell.ShellCommandExecutor(commands.get(i), javaSourceCodeAtRevision);
					try {
						shellMetric.execute();
					} catch (IOException e) { }
					if (shellMetric.getExitCode() == 0) {
						xmlString = shellMetric.getOutput();
						newVal.set(date + "\t" + ParseJavaNCSS.extract(xmlString));
						try {
							out.collect(key, newVal);
						} catch (IOException e) { }
						reporter.incrCounter(Counters.EXIT_0, 1);
					}
					else if (shellMetric.getExitCode() == 1) {
						System.err.println("Cannot get S/W Metric @ " + SHAKey + ":" + javaFile);
					}
					else {
						System.err.println(String.format("<input>%s</input>", javaSourceCodeAtRevision));
						System.err.println(String.format("<exitcode>%s</exitcode>", shellMetric.getExitCode()));
						System.err.println(String.format("<output>%s</output>", shellMetric.getOutput()));						
					}
					reporter.incrCounter(Counters.NUMBER_OF_RECORDS, 1);
				}
			}
			else {
				System.err.print("Command: ");
				for (String c : command)
					System.err.print(c + " ");
				System.err.println();
				System.err.println(String.format("<exitcode>%s</exitcode>", shellRevisionFile.getExitCode()));
				System.err.println(String.format("<output>%s</output>", shellRevisionFile.getOutput()));
			}
		}
	}
}