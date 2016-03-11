package DCacheJoin;
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



public class DCacheJoinDictionaryMapper  extends Mapper<LongWritable, Text, Text, Text> {
 	String fileName=null, language=null;
	   public Map<String, String> translations = new HashMap<String, String>();
	      
	   public void setup(Context context) throws IOException, InterruptedException{
		// TODO: determine the name of the additional language based on the file name    	  
	        // TODO: OPTIONAL: depends on your implementation -- create a HashMap of translations (word, part of speech, translations) from output of exercise 1
	      
	   }
	   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO: perform a map-side join between the word/part-of-speech from exercise 1 and the word/part-of-speech from the distributed cache file

  	       // TODO: where there is a match from above, add language:translation to the list of translations in the existing record (if no match, add language:N/A
	      }

   }
