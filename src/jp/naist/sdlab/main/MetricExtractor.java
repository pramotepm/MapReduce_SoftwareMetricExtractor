package jp.naist.sdlab.main;

import java.net.URI;

import jp.naist.sdlab.Map.MapperGetSHAKeyJavaFile;
import jp.naist.sdlab.Reduce.ReducerGetJavaFileMetric;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MetricExtractor extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), MetricExtractor.class);
		FileSystem fs = FileSystem.get(conf);

		conf.setJobName("test_getJavaFile2");

		// Output Class
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		// MapReduce
		conf.setMapperClass(MapperGetSHAKeyJavaFile.class);
		conf.setReducerClass(ReducerGetJavaFileMetric.class);
		conf.setPartitionerClass(TotalOrderPartitioner.class);
				
		// Input - Output Format
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		int numArgs = 1;
		for (int i=0; i<args.length; i++) {
			if ("-args".equals(args[i]))
				conf.set("metric.args." + numArgs, args[++i]);
			else if ("-main-class".equals(args[i])) {
				conf.set("metric.main.class." + numArgs, args[++i]);
				numArgs++;
			}
			else if ("-required-jar".equals(args[i])) {
				String rj = args[++i];
				for (String jarFile : rj.split(",")) {
					DistributedCache.addFileToClassPath(new Path(jarFile), conf, FileSystem.get(conf));
				}
			}
			else if ("-git".equals(args[i]))
				DistributedCache.addCacheFile(new URI(args[++i]), conf);
		}
		
		// Input - Output Path
		FileOutputFormat.setOutputPath(conf, new Path("/hdfs/birt/output"));
		FileInputFormat.setInputPaths(conf, new Path("/hdfs/birt/input"));

		Preprocess.getTempInputFile(fs, "/home/hadoop/repository/BIRT_R4.2.2");
		
		JobClient.runJob(conf);		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new MetricExtractor(), args);
		System.exit(res);
	}
}