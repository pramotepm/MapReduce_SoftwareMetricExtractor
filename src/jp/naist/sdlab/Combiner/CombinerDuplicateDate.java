package jp.naist.sdlab.Combiner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CombinerDuplicateDate extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	private class Data {
		public GitDate gd;
		public String SHAKey;
		public String javaFilePath;
		
		public Data(GitDate gd, String SHAKey, String javaFilePath) {
			this.gd = gd;
			this.SHAKey = SHAKey;
			this.javaFilePath = javaFilePath;
		}
		
		public String toString() {
			return gd.getShortDate() + "\t" + SHAKey + "\t" + javaFilePath;			
		}
	}
	
	@Override
	public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		Map<String, Data> duplicateDate = new HashMap<String, Data>();
		while (value.hasNext()) {
			String[] temp = value.next().toString().split("\t");
			Data d = new Data(new GitDate(temp[0]), temp[1], temp[2]);
			if (duplicateDate.containsKey(d.gd.getShortDate())) {
				if (!duplicateDate.get(d.gd.getShortDate()).gd.isDateFresherThan(d.gd))
					duplicateDate.put(d.gd.getShortDate(), d);	
			}
			else
				duplicateDate.put(d.gd.getShortDate(), d);
		}
		Text val = new Text();
		for (Data d : duplicateDate.values()) {
			val.set(d.toString());
			out.collect(key, val);
		}
	}
}