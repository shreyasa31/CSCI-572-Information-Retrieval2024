import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();
    private Text docID = new Text();

    // Using an array of phrases to maintain order
    private static final String[] orderedPhrases = {
        "computer science",
        "information retrieval",
        "power politics",
        "los angeles",
        "bruce willis"
    };

    // Mapping each phrase to its order index
    private static final HashMap<String, Integer> phrasesOrder = new HashMap<>();
    static {
      for (int i = 0; i < orderedPhrases.length; i++) {
        phrasesOrder.put(orderedPhrases[i], i);
      }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] doc = value.toString().split("\t", 2); // Splitting the line into docID and docContent

      // Ensure there are at least 2 parts: docID and docContent
      if (doc.length < 2)
        return;

      String docIDString = doc[0];
      String docContent = doc[1].toLowerCase(); // Convert content to lower case
      docContent = docContent.replaceAll("[^a-z\\s]", " "); // replacing all non-alphanumeric characters with space
      docContent = docContent.replaceAll("\\s+", " "); // collapsing multiple spaces into one

      docID.set(docIDString); // setting the docID

      // Split the document content into words
      String[] words = docContent.split("\\s+");

      // Iterate through each phrase to find duplicates
      for (String phrase : orderedPhrases) {
        String[] phraseWords = phrase.split("\\s+");
        // Search for the phrase in the words array
        for (int i = 0; i <= words.length - phraseWords.length; i++) {
          boolean match = true;
          for (int j = 0; j < phraseWords.length; j++) {
            if (!words[i + j].equals(phraseWords[j])) {
              match = false;
              break;
            }
          }
          if (match) {
            // Phrase found, emit it with its order index
            String prefixedPhrase = String.format("%02d_%s", phrasesOrder.get(phrase), phrase);
            word.set(prefixedPhrase);
            context.write(word, docID);
          }
        }
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    private Text finalResult = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // HashMap to accumulate counts for each docID
      HashMap<String, Integer> count = new HashMap<>();
      for (Text val : values) {
        String docID = val.toString();
        // Increment count for the docID
        count.put(docID, count.getOrDefault(docID, 0) + 1);
      }

      // Prepare the output text
      StringBuilder stringBuilder = new StringBuilder();
      for (Map.Entry<String, Integer> entry : count.entrySet()) {
        stringBuilder.append(entry.getKey()).append(":").append(entry.getValue()).append("\t");
      }
      String result = stringBuilder.toString();
      if (!result.isEmpty()) {
        result = result.substring(0, result.length() - 1); // Remove the last tab character
      }
      finalResult.set(result);

      // Remove the order index prefix from the key before writing the output
      String originalKey = key.toString().substring(key.toString().indexOf('_') + 1);
      Text originalKeyText = new Text(originalKey);

      // Write the result using the original key
      context.write(originalKeyText, finalResult);
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");

    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);

    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// WordCount