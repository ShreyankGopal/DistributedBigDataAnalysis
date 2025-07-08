import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import javax.naming.Context;

import java.io.BufferedWriter;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class WordCount2 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private boolean caseSensitive = false;
        private Set<String> patternsToSkip = new HashSet<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI uri : cacheFiles) {
                    parseStopWordsFile(new Path(uri.getPath()).getName());
                }
            }
        }

        private void parseStopWordsFile(String fileName) {
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    patternsToSkip.add(line.trim().toLowerCase());
                }
            } catch (IOException e) {
                System.err.println("Error reading stop words file: " + StringUtils.stringifyException(e));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = caseSensitive ? value.toString() : value.toString().toLowerCase();
            String[] tokens = line.split("[^\\w']+");  // Split words on non-alphanumeric characters

            for (String token : tokens) { 
                if (!token.isEmpty() && !patternsToSkip.contains(token)) {  // Skip stop words
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private PriorityQueue<WordCount> topWords = new PriorityQueue<>(Comparator.comparingInt(wp -> wp.count));

        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            topWords.offer(new WordCount(new Text(key), sum));
            if (topWords.size() > 50) {
                topWords.poll();  // Keep only top 50
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<WordCount> sortedWords = new ArrayList<>(topWords);
            sortedWords.sort((a, b) -> Integer.compare(b.count, a.count));  // Sort descending order

            for (WordCount wp : sortedWords) {
                context.write(wp.word, new IntWritable(wp.count));
            }
        }

        private static class WordCount {
            Text word;
            int count;

            WordCount(Text word, int count) {
                this.word = word;
                this.count = count;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top 50 word count");

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < args.length; ++i) {
            if ("-skippatterns".equals(args[i])) {
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
                job.addCacheFile(new Path(args[++i]).toUri());
            } else if ("-casesensitive".equals(args[i])) {
                job.getConfiguration().setBoolean("wordcount.case.sensitive", true);
            }
        }
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
