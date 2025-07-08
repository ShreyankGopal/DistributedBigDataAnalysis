import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class Question4c {
    
    public static class StripeMapper extends Mapper<Object, Text, Text, MapWritable> {
        private Set<String> topWords = new HashSet<>();
        private Text wordKey = new Text();
        private int distance;
        
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            distance = conf.getInt("distance", 0);
            URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI uri : cacheFiles) {
                    loadTopWords(new Path(uri.getPath()).getName());
                }
            }
        }
        
        private void loadTopWords(String fileName) {
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\t");
                    if (parts.length > 0) {
                        topWords.add(parts[0]);
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading top words file: " + StringUtils.stringifyException(e));
            }
        }
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            String[] tokens = line.split("[^\\w']+");
            
            // Use a sliding window of size 2*distance+1 centered at current word
            // For each word in the window, create a stripe (map of neighbors)
            for (int i = 0; i < tokens.length; i++) {
                if (!topWords.contains(tokens[i])) {
                    continue; // Skip if not in top words list
                }
                
                // Create a map for co-occurrences for this word
                MapWritable stripe = new MapWritable();
                
                // Look backward within distance
                for (int j = Math.max(0, i - distance); j < i; j++) {
                    if (topWords.contains(tokens[j])) {
                        incrementStripe(stripe, tokens[j]);
                    }
                }
                
                // Look forward within distance
                for (int j = i + 1; j <= Math.min(tokens.length - 1, i + distance); j++) {
                    if (topWords.contains(tokens[j])) {
                        incrementStripe(stripe, tokens[j]);
                    }
                }
                
                // Only emit if there are co-occurrences
                if (!stripe.isEmpty()) {
                    wordKey.set(tokens[i]);
                    context.write(wordKey, stripe);
                }
            }
        }
        
        private void incrementStripe(MapWritable stripe, String coWord) {
            Text coWordText = new Text(coWord);
            IntWritable count = (IntWritable) stripe.get(coWordText);
            if (count == null) {
                stripe.put(coWordText, new IntWritable(1));
            } else {
                count.set(count.get() + 1);
            }
        }
    }
    
    public static class StripeReducer extends Reducer<Text, MapWritable, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) 
                throws IOException, InterruptedException {
            // Aggregate stripes for the same word
            MapWritable aggregateStripe = new MapWritable();
            
            for (MapWritable stripe : values) {
                // Merge each stripe into the aggregate
                for (Writable coWord : stripe.keySet()) {
                    IntWritable count = (IntWritable) stripe.get(coWord);
                    IntWritable existingCount = (IntWritable) aggregateStripe.get(coWord);
                    
                    if (existingCount == null) {
                        aggregateStripe.put(coWord, new IntWritable(count.get()));
                    } else {
                        existingCount.set(existingCount.get() + count.get());
                    }
                }
            }
            
            // Format output as "word \t coword1=count1 coword2=count2 ..."
            int size = aggregateStripe.size();
            int occ=0;
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (Writable coWord : aggregateStripe.keySet()) {
                IntWritable count = (IntWritable) aggregateStripe.get(coWord);
                sb.append(coWord.toString()).append("=").append(count.get());
                if (occ < size - 1) {
                    sb.append(", ");
                }
                occ++;
            }
            sb.append("}");
            context.write(key, new Text(sb.toString().trim()));
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Co-occurring Word Matrix (Stripe)");
        
        job.setJarByClass(Question4c.class);
        job.setMapperClass(StripeMapper.class);
        job.setReducerClass(StripeReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Set distance parameter
        job.getConfiguration().setInt("distance", Integer.parseInt(args[3]));
        
        // Add the top words file to distributed cache
        job.addCacheFile(new Path(args[2]).toUri());
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
