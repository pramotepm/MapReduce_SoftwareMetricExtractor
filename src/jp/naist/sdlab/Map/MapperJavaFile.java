package jp.naist.sdlab.Map;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import jp.naist.sdlab.utils.FileSystemReader;
import jp.naist.sdlab.utils.JarRunnableManager;
import jp.naist.sdlab.utils.ShellUnixCommandExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapperJavaFile extends MapReduceBase implements Mapper<NullWritable, Text, Text, Text> {

	enum Counters { STAGE };

	private Configuration jobconf;
	
	public List<List<String>> getExecuteCommand() {
		JarRunnableManager jM = new JarRunnableManager(this.jobconf);
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
			
			String y = "";
			for(String x : command) {
				y += x + " ";
			}
		
			System.out.println(y);
		
		}
		return commands;
	}
	
	public String[] getMetrics(String input) throws IOException {
		List<List<String>> commands = getExecuteCommand();
		String output = null;
		for (int i=0; i<commands.size(); i++) {
			ShellUnixCommandExecutor shell = new ShellUnixCommandExecutor(commands.get(i), FileSystemReader.readAll(new Path(input), jobconf));
			shell.execute();
			output = shell.getOutput();
		}
		return new String[] { output, "y" };
	}

	public String getJavaFileAbsolutePackage(String javaFilePath) {
		List<String> temp = new LinkedList<String>();
		boolean start = false;
		StringTokenizer tokens = new StringTokenizer(javaFilePath.toString(), "/");
		while(tokens.hasMoreTokens()) {
			String token = tokens.nextToken();
			if (start)
				temp.add(token);
			if (token.equals("src"))
				start = true;
		}
		if (temp.size() > 0) {
			StringBuilder javaAbsPath = new StringBuilder(temp.get(0));
			for (int i=1; i<temp.size(); i++) {
				javaAbsPath.append(".");
				javaAbsPath.append(temp.get(i));
			}
			return javaAbsPath.toString();
		}
		else
			return null;
	}

	@Override
	public void configure(JobConf job) {
		this.jobconf = job;
	}

	@Override
	public void map(NullWritable key, Text val, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		System.out.println(val.toString());
		String[] x = getMetrics(val.toString());
		out.collect(new Text("<key>" + getJavaFileAbsolutePackage(val.toString()) + "</key>"), new Text("<value>" + x[0] + "</value>"));
		reporter.setStatus("exit code = " + x[1]);
	}
}