package jp.naist.sdlab.Map;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

public class PassingRecordReader implements RecordReader<Text, Text> {

	private FileSplit fileSplit;
	private boolean processed = false;

	public PassingRecordReader(FileSplit fileSplit, Configuration conf) {
		this.fileSplit = fileSplit;
	}
	
	@Override
	public void close() throws IOException {
		;
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		return processed ? fileSplit.getLength() : 0;
	}

	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f; 
	}

	@Override
	public boolean next(Text key, Text value) throws IOException {
		if (!processed) {
			Path file = fileSplit.getPath();
			value.set(file.toString());
			processed = true;
			return true;
		}
		return false;
	}
}