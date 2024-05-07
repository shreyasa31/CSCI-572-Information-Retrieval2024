import java.io.IOException;
import java.util.HashMap;
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

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] doc = value.toString().split("\t", 2); // splitting the line into docID and docContent

      // converting the docContent to lower case and replacing all non-alphanumeric
      // characters with a space

      String text = doc[1].toLowerCase();
      text = text.replaceAll("[^a-z\\s]", " ");
      text = text.replaceAll("\\s+", " ");

      docID.set(doc[0]);// setting the docID

      StringTokenizer tokens = new StringTokenizer(text); // setting word
      while (tokens.hasMoreTokens()) {
        word.set(tokens.nextToken());
        context.write(word, docID);
      }

    }
  }

  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    private Text finalResult = new Text(); // setting the final result

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      HashMap<String, Integer> count = new HashMap<>(); // creating a hashmap to store the word and its count
      // iterating through the values
      for (Text a : values) {
        String docID = a.toString();
        count.put(docID, count.getOrDefault(docID, 0) + 1);
      }

      StringBuilder s = new StringBuilder();// creating a string builder to store the final result
      for (String k : count.keySet()) { // iterating through the hashmap

        s.append(k).append(":").append(count.get(k)).append("\t"); // appending the key and value to the string
      }
      finalResult.set(s.substring(0, s.length() - 1)); // setting the final result by excluding the last tab space
      context.write(key, finalResult); // writing the final result
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");

    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);

    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}// WordCount
