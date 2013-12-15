package jp.naist.sdlab.Reduce;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import jp.naist.sdlab.utils.JarRunnableManager;

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
	
	private List<String[]> getExecuteCommand() {
		JarRunnableManager jM = new JarRunnableManager(conf);
		List<String[]> commands = new LinkedList<String[]>();
		for (int i=0; i<jM.getNumberOfCommand(); i++) {
			List<String> command = new LinkedList<String>();
			command.add("java");
			command.add("-classpath");
			command.add(jM.getClassPath());
			command.add(jM.getMainClass()[i]);
			for (String arg : jM.getArguments()[i].split(","))
				command.add(arg);
			commands.add(command.toArray(new String[command.size() + 1]));
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
		List<String[]> commands = getExecuteCommand();
		TempFile tempFile = new TempFile();
		
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
				StringBuilder metricOutput = new StringBuilder("");
				String javaSourceCodeAtRevision = shellRevisionFile.getOutput();
				tempFile.write(javaSourceCodeAtRevision);
				
				for (int libIterator=0; libIterator<commands.size(); libIterator++) {
					String[] libCommand = commands.get(libIterator);
					libCommand[libCommand.length - 1] = tempFile.getPath();

					ShellCommandExecutor shellMetric = new ShellCommandExecutor(libCommand);
					try {
						shellMetric.execute();
					} catch (IOException e) { }
					if (shellMetric.getExitCode() == 0) {
						String toolsOutput = shellMetric.getOutput();
						if (toolsOutput == null || toolsOutput.equals("")) {
							String err = "";
							for (int i=0; i<libCommand.length; i++)
								err += libCommand[i] + " ";
							System.err.println(String.format("<command>%s</command>", err));
							continue;
						}
						switch(libIterator) {
							case 0: {
								metricOutput.append(ParseJavaNCSS.parse(toolsOutput));
							} break;
							case 1: {
								metricOutput.append(ParseLOC.parse(toolsOutput));
							} break;
							case 2: {
								metricOutput.append(ParseJRefactory.parse(toolsOutput));
							} break;
						}
						reporter.incrCounter(Counters.EXIT_0, 1);
					}
					else if (shellMetric.getExitCode() == 1) {
						System.err.println("Error: Library#" + libIterator + " with " + SHAKey + ":" + javaFile);
					}
					else {
						System.err.println(String.format("<input>%s</input>", javaSourceCodeAtRevision));
						System.err.println(String.format("<exitcode>%s</exitcode>", shellMetric.getExitCode()));
						System.err.println(String.format("<output>%s</output>", shellMetric.getOutput()));						
					}
				}
				if (!metricOutput.toString().equals("")) {
					try {
						newVal.set(date + ";" + metricOutput.toString());
						out.collect(key, newVal);
					} catch (IOException e) { 
						e.printStackTrace();
					}
				}
				reporter.incrCounter(Counters.NUMBER_OF_RECORDS, 1);
			}
			else {
				if (shellRevisionFile.getExitCode() == 128)
					continue;
				System.err.print("Command: ");
				for (String c : command)
					System.err.print(c + " ");
				System.err.println();
				System.err.println(String.format("<exitcode>%s</exitcode>", shellRevisionFile.getExitCode()));
				System.err.println(String.format("<output>%s</output>", shellRevisionFile.getOutput()));
			}
		}
		tempFile.close();
	}
}