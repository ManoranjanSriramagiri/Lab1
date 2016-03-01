package Dictionary;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DictionaryReducer extends Reducer<Text,Text,Text,Text> {
	 private Text result = new Text();
   public void reduce(Text word, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
	   String translations = "";
	   
       for (Text val : values) {
           translations += "|" + val.toString();
       }

       result.set(translations);
       context.write(word, result);
   }
}
