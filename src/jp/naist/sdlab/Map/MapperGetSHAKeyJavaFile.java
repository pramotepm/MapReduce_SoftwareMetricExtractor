package jp.naist.sdlab.Map;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

public class MapperGetSHAKeyJavaFile extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
	public String locateGitDir(Path[] distribPaths) {
		for (Path path : distribPaths) {
			if (path.getName().endsWith(".git"))
				return path.toString();
		}
		return null;
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		String[] tmp = value.toString().split("\t");
		String id = tmp[0];
		String javaFile = tmp[1];
		String gitDir = new File(".git").getAbsolutePath();
		String[] command = new String[] { "git", "--git-dir=" + gitDir, "log", "--format=\"%H %ad\"", "--", javaFile };
		
		ShellCommandExecutor shell = new ShellCommandExecutor(command);
		shell.execute();
		if (shell.getExitCode() == 0) {
			String outputs = shell.getOutput();
			String date = null;
			String SHAKey = null;
			Text tID = new Text(id);
			Text tVal = new Text();
			for (String record : outputs.split(System.getProperty("line.separator"))) {
				String[] field = record.replaceAll("\"", "").split("\\s", 2);
				SHAKey = field[0];
				date = field[1];
				tVal.set(date + "\t" + SHAKey + "\t" + javaFile);
				out.collect(tID, tVal);
			}
		}
		else {
			System.err.print("Command: ");
			for (String c : command)
				System.err.print(c + " ");
			System.err.println();
			System.err.println(String.format("<exitcode>%s</exitcode>", shell.getExitCode()));
			System.err.println(String.format("<output>%s</output>", shell.getOutput()));
		}
	}
}