package jp.naist.sdlab.main;

import jp.naist.sdlab.Combiner.CombinerDuplicateDate;
import jp.naist.sdlab.Map.MapperGetSHAKeyJavaFile;
import jp.naist.sdlab.Reduce.ReducerGetJavaFileMetric;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MetricExtractor extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), MetricExtractor.class);
		FileSystem fs = FileSystem.get(conf);

		// Output Class
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		// MapReduce
		conf.setMapperClass(MapperGetSHAKeyJavaFile.class);
		conf.setCombinerClass(CombinerDuplicateDate.class);
		conf.setReducerClass(ReducerGetJavaFileMetric.class);
				
		// Input - Output Format
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.set("mapred.textoutputformat.separator", ";");
		
		String inputRepository = null;
		String inputPath = null;
		String outputPath = null;
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
			else if ("-input-path".equals(args[i]))
				inputPath = args[++i];
			else if ("-output-path".equals(args[i]))
				outputPath = args[++i];
			else if ("-input-repository".equals(args[i]))
				inputRepository = args[++i];
			else if ("-n".equals(args[i]))
				conf.set("preprocess.file.size", args[++i]);
		}
		
		// Input - Output Path
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		Preprocess.getTempInputFile(fs, inputRepository);
		
		JobClient.runJob(conf);		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res =  ToolRunner.run(new Configuration(), new MetricExtractor(), args);
		System.exit(res);
	}
}