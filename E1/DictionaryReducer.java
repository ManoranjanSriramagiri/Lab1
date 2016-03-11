package Dictionary;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DictionaryReducer extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();

	public void reduce(Text word, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String translations = "";
		String french = "N/A ", german = "N/A ", italian = "N/A ", portuguese = "N/A ", spanish = "N/A ";
		int[] a = { 0, 0, 0, 0, 0 };
		if (word.toString() != null || word.toString() != "") {
			for (Text val : values) {

				{
					try {
						if (val.toString().substring(0, 6).equals("french")) {
							if (a[0] == 0) {
								french = val.toString().substring(7,
										val.toString().length());
								a[0]++;
							} else {
								french = val.toString().substring(7,
										val.toString().length())
										+ ", " + french;
							}

						} else if (val.toString().substring(0, 6)
								.equals("german")) {
							if (a[1] == 0) {
								german = val.toString().substring(7,
										val.toString().length());
								a[1]++;
							} else {
								german = val.toString().substring(7,
										val.toString().length())
										+ ", " + german;
							}
						} else if (val.toString().substring(0, 7)
								.equals("italian")) {
							if (a[2] == 0) {
								italian = val.toString().substring(8,
										val.toString().length());
								a[2]++;
							} else {
								italian = val.toString().substring(8,
										val.toString().length())
										+ ", " + italian;
							}
						} else if (val.toString().substring(0, 10)
								.equals("portuguese")) {
							if (a[3] == 0) {
								portuguese = val.toString().substring(11,
										val.toString().length());
								a[3]++;
							} else {
								portuguese = val.toString().substring(11,
										val.toString().length())
										+ ", " + portuguese;
							}
						} else if (val.toString().substring(0, 7)
								.equals("spanish")) {

							if (a[4] == 0) {
								spanish = val.toString().substring(8,
										val.toString().length());
								a[4]++;
							} else {
								spanish = val.toString().substring(8,
										val.toString().length())
										+ ", " + spanish;
								a[4]++;
							}
						}

					} catch (Exception e) {
						
					}

				}
			}

			french = "french: " + french + " |";

			german = "german: " + german + " |";

			italian = "italian: " + italian + " |";

			portuguese = "portuguese: " + portuguese + " |";

			spanish = "spanish: " + spanish;

			translations = french + german + italian + portuguese + spanish;

			result.set(translations);
			if (word.toString().length() > 0) {
				if ((int) word.toString().charAt(0) >= 97
						&& (int) word.toString().charAt(0) <= 122)
					context.write(word, result);
			}

		} else {
			
		}

	}
}
