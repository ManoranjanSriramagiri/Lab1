package DCacheJoin;

import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DCacheJoinDictionaryMapper extends Mapper<Text, Text, Text, Text> {
	String fileName = null, language = null;
	FileReader inputFile;
	public Map<String, String> translations = new HashMap<String, String>();

	public void setup(Context context) throws IOException, InterruptedException {
		inputFile = new FileReader("latin.txt");
		
	}

	public void FileToTranslations(Context context) throws IOException, InterruptedException {
		BufferedReader bufferReader = new BufferedReader(inputFile);
		//String line;
		Stream <String> lines=bufferReader.lines();
		Iterator<String> itr = lines.iterator();
		while (itr.hasNext()) 
		{
			String read=itr.next();
			String[] parts=read.split(" ", 2);
			String line = parts[1];
			String key =parts[0];
			
			int length = line.length();
	        
			
			String oldKey;
			Text englishenglishWordMeaningValue = new Text();
			String partOfSpeech=parts[1];
			String englishenglishWordMeaningValueMeaning;
			int len;
			int startOfPos = 0;

			partOfSpeech = line.toString();
			len = partOfSpeech.length();

			try {

				if (partOfSpeech.charAt(len - 1) == ']') {

					for (int i = len - 1; i > 0; i--) {

						if (partOfSpeech.charAt(i) == '[') {

							startOfPos = i;

						}
					}
					if ((partOfSpeech.substring(startOfPos + 1, len - 1)
							.equalsIgnoreCase("Adjective"))
							|| (partOfSpeech.substring(startOfPos + 1, len - 1)
									.equalsIgnoreCase("Adverb"))
							|| (partOfSpeech.substring(startOfPos + 1, len - 1)
									.equalsIgnoreCase("Conjunction"))
							|| (partOfSpeech.substring(startOfPos + 1, len - 1)
									.equalsIgnoreCase("Noun"))
							|| (partOfSpeech.substring(startOfPos + 1, len - 1)
									.equalsIgnoreCase("Preposition"))
							|| (partOfSpeech.substring(startOfPos + 1, len - 1)
									.equalsIgnoreCase("Pronoun"))
							|| (partOfSpeech.substring(startOfPos + 1, len - 1)
									.equalsIgnoreCase("Verb"))

					) {

						oldKey = partOfSpeech.substring(startOfPos, len).trim();
						englishenglishWordMeaningValueMeaning = partOfSpeech.substring(1, startOfPos);

						englishenglishWordMeaningValueMeaning = " |latin" + ":" + englishenglishWordMeaningValueMeaning;
						englishenglishWordMeaningValue.set(key + ":" + oldKey);

						
						translations.put(key + ":" + oldKey, englishenglishWordMeaningValueMeaning);
					}
			
				} 
				else 
				{

				}
			} 
			catch (Exception e) {
			}}
	}

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		FileToTranslations(context);
		if (translations.containsKey(key.toString())) 
		{
			String x=translations.get(key.toString())+value.toString();
			Text newVal =new Text();
			newVal.set(x);
			context.write(key, newVal);
			translations.remove(key);
					
			
		} 
		else 
		{
			String x=translations.get(key.toString())+value.toString();
			Text newVal =new Text();
			newVal.set(x+" |latin:N/A");
			context.write(key, newVal);

		}
		String na="french:N/A |german:N/A |italian:N/A |portuguese:N/A |spanish:N/A";
		for (Map.Entry<String, String> rem : translations.entrySet()) {
			
			context.write(new Text(rem.getKey()), new Text(na+rem.getValue()));
			
		}
	}

}
