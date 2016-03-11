package Dictionary;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DictionaryMapper extends Mapper<Text, Text, Text, Text> {
	// TODO define class variables for translation, language, and file name
	String filename;

	public void setup(Context context) {
		filename = ((FileSplit) context.getInputSplit()).getPath().getName()
				.toString();
		filename = filename.substring(0, filename.length() - 4);
	}

	private Text englishWordMeaningValue = new Text();

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		String oldKey;
		Text englishenglishWordMeaningValue = new Text();
		String partOfSpeech;
		String englishenglishWordMeaningValueMeaning;
		int len;
		int startOfPos = 0;

		partOfSpeech = value.toString();
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

					englishenglishWordMeaningValueMeaning = filename + ":" + englishenglishWordMeaningValueMeaning;
					englishenglishWordMeaningValue.set(key + ":" + oldKey);

					englishWordMeaningValue.set(englishenglishWordMeaningValueMeaning);
					context.write(englishenglishWordMeaningValue, englishWordMeaningValue);
				}

			} 
			else 
			{

			}
		} 
		catch (Exception e) {
		}
	}

}