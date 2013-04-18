package jp.naist.sdlab.main;

import jp.naist.sdlab.Map.MapperJavaFile;
import jp.naist.sdlab.Map.NonSplittableFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MetricExtractor extends Configured implements Tool {
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
		conf.setMapperClass(MapperJavaFile.class);
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
				conf.set("metric.main.class." + numArgs, args[i]);
			}
			else if ("-required-jar".equals(args[i])) {
				String rj = args[++i];
				for (String jarFile : rj.split(",")) {
					DistributedCache.addFileToClassPath(new Path(jarFile), conf, FileSystem.get(conf));
				}
			}
			numArgs++;
		}
		
		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MetricExtractor(), args);
		System.exit(res);
	}
}