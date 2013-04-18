package jp.naist.sdlab.Map;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class NonSplittableFileInputFormat extends FileInputFormat<Text, Text> {
	
	private Set<Path> listFiles(FileSystem fs, Path p) throws IOException {
		Set<Path> setOfPath = new HashSet<Path>();
		Path[] listPath = FileUtil.stat2Paths(fs.listStatus(p));
		for (Path path : listPath) {
			if (fs.isFile(path)) {
				if (path.toString().endsWith(".java")) {
					setOfPath.add(path);
				}
			}
			else {
				setOfPath.addAll(listFiles(fs, path));
			}
		}
		return setOfPath;
	}
	
	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}

	@Override
	protected FileStatus[] listStatus(JobConf conf) throws IOException {
		String inputFile = conf.get("mapred.input.dir");
		FileSystem fs = FileSystem.get(conf);
		Set<Path> s = listFiles(fs, new Path(inputFile));
		return fs.listStatus(s.toArray(new Path[s.size()]));
	}

	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
		return new PassingRecordReader((FileSplit) split, job);
	}
}