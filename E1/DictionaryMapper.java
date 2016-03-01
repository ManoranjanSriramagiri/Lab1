package Dictionary;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;

public class DictionaryMapper extends Mapper<Text, Text, Text, Text> {
	// TODO define class variables for translation, language, and file name
	// dklnldsnln
	/*
	 * public void setup(Context context) { Configuration conf =
	 * context.getConfiguration(); }
	 */
	private Text word = new Text();

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString(), ",");
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(key, word);
		}
	}
}
