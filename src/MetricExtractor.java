import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MetricExtractor extends Configured implements Tool {
	public static class JavaFileMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

		static enum Counters { STAGE }
		
		private Configuration jobconf;
		private String[] args;
		private String[] distFiles;
		private String classpath = "";
		
		public String[] executeMetricGetter(String metricProvider, String args, String input, Reporter r) throws IOException {
			// metricProvider
			String localdir = ((JobConf) jobconf).getJobLocalDir();
			System.out.println(">>> " + localdir);
			FileSystem fs = FileSystem.get(jobconf);
			fs.copyToLocalFile(new Path(input), new Path(localdir));
			String newInputPath = localdir + "/" + new Path(input).getName();
			System.out.println(">>> new >>> " + newInputPath);
			
			List<String> _cmd = new LinkedList<String>();
			_cmd.add("java");
			_cmd.add("-classpath");
			_cmd.add(classpath);
			//_cmd.add("-jar");
			//_cmd.add(metricProvider);
			for (String s : args.split(","))
				_cmd.add(s);
			_cmd.add(newInputPath);
			String[] cmd = _cmd.toArray(new String[_cmd.size()]);
			
			for (String c : cmd)
				System.out.println("> " + c);
						
			ShellCommandExecutor shell = new ShellCommandExecutor(cmd);
			shell.execute();
			
			return new String[] { shell.getOutput(), String.valueOf(shell.getExitCode()) };
		}
		
		public String findMainClass(Path[] p, String mainClassName) {
			for (Path _p : p)
				if (_p.toString().contains(mainClassName))
					return _p.toString();
			return mainClassName;
		}
		
		@Override
		public void configure(JobConf job) {
			this.jobconf = job;
			
			Collection<String> temp1 = this.jobconf.getValByRegex("metric.main.class.*").values();
			this.distFiles = temp1.toArray(new String[temp1.size()]);

			try {
				for (int i=0; i<this.distFiles.length; i++)
					this.distFiles[i] = findMainClass(DistributedCache.getLocalCacheFiles(job), this.distFiles[i]);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			try {
				Path[] p = DistributedCache.getLocalCacheFiles(job);
				if (p.length > 0) {
					classpath += p[0].toString();
					for (int i=1; i<p.length; i++) {
						System.out.println(p[i]);
						classpath += ":" + p[i];
					}
				}
				System.out.println(classpath);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			Collection<String> _temp = job.getValByRegex("metric.args.*").values();
			this.args = _temp.toArray(new String[_temp.size()]);
			
		}

		@Override
		public void map(Text key, Text val, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
			List<String> temp = new LinkedList<String>();
			boolean start = false;
			
			StringTokenizer tokens = new StringTokenizer(key.toString(), "/");
			while(tokens.hasMoreTokens()) {
				String token = tokens.nextToken();
				if (start)
					temp.add(token);
				if (token.equals("src"))
					start = true;
			}
			
			if (temp.size() > 0) {
				StringBuilder _newKey = new StringBuilder(temp.get(0));
				for (int i=1; i<temp.size(); i++) {
					_newKey.append(".");
					_newKey.append(temp.get(i));
				}
				String newKey = _newKey.toString();
				
				for (int i=0; i<distFiles.length; i++) {
					String[] x = executeMetricGetter(distFiles[i], args[i], val.toString(), reporter);
					out.collect(new Text("<key>" + newKey + "</key>"), new Text("<value>" + x[0] + "</value>"));
					reporter.setStatus("exit code = " + x[1]);
				}
				//report.setStatus("Key: " + newKey + "\t" + "Value: " + val.toString());
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), MetricExtractor.class);
		
		conf.setJobName("test_getJavaFile");
		
		// Input - Output Format
		conf.setInputFormat(NonSplittableFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		// Output Class
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		// MapReduce
		conf.setMapperClass(JavaFileMapper.class);
		conf.setReducerClass(IdentityReducer.class);
		
		// Input - Output Path
		FileInputFormat.setInputPaths(conf, new Path("/hdfs/BIRT_PROJECT/small_input"));
		FileOutputFormat.setOutputPath(conf, new Path("/hdfs/BIRT_PROJECT/output"));
		
		int numArgs = 1;
		for (int i=0; i<args.length; i++) {
			if ("-args".equals(args[i]))
				conf.set("metric.args." + numArgs, args[++i]);
			else if ("-main-class".equals(args[i])) {
				i++;
				//DistributedCache.addCacheFile(new Path(args[i]).toUri(), conf);
				conf.set("metric.main.class." + numArgs, args[i]);
			}
			else if ("-required-jar".equals(args[i])) {
				String rj = args[++i];
				for (String jarFile : rj.split(",")) {
					DistributedCache.addFileToClassPath(new Path(jarFile), conf, FileSystem.get(conf));
					//DistributedCache.addCacheFile(new Path(jarFile).toUri(), conf);
				}
			}
		}
		
		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MetricExtractor(), args);
		System.exit(res);
	}
}