// import java.io.BufferedReader;
// import java.io.FileReader;
// import java.io.IOException;
// import java.net.URI;
// import java.util.*;

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.io.Writable;
// import org.apache.hadoop.io.WritableComparable;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.util.StringUtils;

// public class Question4e {
    
//     // Same WordPair class as in Question4b
//     static class WordPair implements WritableComparable<WordPair> {
//         private Text word1;
//         private Text word2;

//         public WordPair() {
//             this.word1 = new Text();
//             this.word2 = new Text();
//         }

//         public WordPair(Text word1, Text word2) {
//             this.word1 = word1;
//             this.word2 = word2;
//         }

//         public Text getWord1() {
//             return word1;
//         }

//         public Text getWord2() {
//             return word2;
//         }
        
//         @Override
//         public int compareTo(WordPair o) {
//             String thisValue = this.word1.toString() + " " + this.word2.toString();
//             String thatValue = o.word1.toString() + " " + o.word2.toString();
//             return thisValue.compareTo(thatValue);
//         }
        
//         @Override
//         public void write(java.io.DataOutput out) throws IOException {
//             word1.write(out);
//             word2.write(out);
//         }

//         @Override
//         public void readFields(java.io.DataInput in) throws IOException {
//             word1.readFields(in);
//             word2.readFields(in);
//         }

//         @Override
//         public String toString() {
//             return "{" + word1.toString() + "," + word2.toString() + "}";
//         }
        
//         @Override
//         public int hashCode() {
//             return word1.hashCode() * 163 + word2.hashCode();
//         }
        
//         @Override
//         public boolean equals(Object obj) {
//             if (obj instanceof WordPair) {
//                 WordPair other = (WordPair) obj;
//                 return word1.equals(other.word1) && word2.equals(other.word2);
//             }
//             return false;
//         }
//     }
    
//     /**
//      * Mapper with in-mapper aggregation at class level
//      * This approach maintains state within the mapper instance
//      */
//     public static class ClassLevelAggregationMapper extends Mapper<Object, Text, WordPair, IntWritable> {
//         private Set<String> topWords = new HashSet<>();
//         private Text word1 = new Text();
//         private Text word2 = new Text();
//         private int distance;
        
//         // Map to store aggregated counts within this mapper instance
//         private Map<WordPair, Integer> pairCounts = new HashMap<>();
        
//         @Override
//         public void setup(Context context) throws IOException, InterruptedException {
//             Configuration conf = context.getConfiguration();
//             distance = conf.getInt("distance", 0);
//             URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();
//             if (cacheFiles != null && cacheFiles.length > 0) {
//                 for (URI uri : cacheFiles) {
//                     loadTopWords(new Path(uri.getPath()).getName());
//                 }
//             }
//         }
        
//         private void loadTopWords(String fileName) {
//             try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
//                 String line;
//                 while ((line = reader.readLine()) != null) {
//                     String[] parts = line.split("\\t");
//                     if (parts.length > 0) {
//                         topWords.add(parts[0]);
//                     }
//                 }
//             } catch (IOException e) {
//                 System.err.println("Error reading top words file: " + StringUtils.stringifyException(e));
//             }
//         }
        
//         @Override
//         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//             String line = value.toString().toLowerCase();
//             String[] tokens = line.split("[^\\w']+");
            
//             for (int i = 0; i < tokens.length; i++) {
//                 if (!topWords.contains(tokens[i])) {
//                     continue;
//                 }
                
//                 // Check for co-occurring words within distance
//                 for (int j = Math.max(0, i - distance); j < i; j++) {
//                     if (topWords.contains(tokens[j])) {
//                         word1.set(tokens[i]);
//                         word2.set(tokens[j]);
//                         WordPair pair = new WordPair(new Text(word1), new Text(word2));
//                         pairCounts.put(pair, pairCounts.getOrDefault(pair, 0) + 1);
//                     }
//                 }
                
//                 for (int j = i + 1; j <= Math.min(tokens.length - 1, i + distance); j++) {
//                     if (topWords.contains(tokens[j])) {
//                         word1.set(tokens[i]);
//                         word2.set(tokens[j]);
//                         WordPair pair = new WordPair(new Text(word1), new Text(word2));
//                         pairCounts.put(pair, pairCounts.getOrDefault(pair, 0) + 1);
//                     }
//                 }
//             }
//         }
        
//         @Override
//         protected void cleanup(Context context) throws IOException, InterruptedException {
//             // Emit aggregated counts at the end of the mapper's lifecycle
//             for (Map.Entry<WordPair, Integer> entry : pairCounts.entrySet()) {
//                 context.write(entry.getKey(), new IntWritable(entry.getValue()));
//             }
//         }
//     }
    
//     /**
//      * Mapper with function-level aggregation
//      * This approach aggregates counts within each map function call
//      */
//     public static class FunctionLevelAggregationMapper extends Mapper<Object, Text, WordPair, IntWritable> {
//         private Set<String> topWords = new HashSet<>();
//         private Text word1 = new Text();
//         private Text word2 = new Text();
//         private int distance;
        
//         @Override
//         public void setup(Context context) throws IOException, InterruptedException {
//             Configuration conf = context.getConfiguration();
//             distance = conf.getInt("distance", 0);
//             URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();
//             if (cacheFiles != null && cacheFiles.length > 0) {
//                 for (URI uri : cacheFiles) {
//                     loadTopWords(new Path(uri.getPath()).getName());
//                 }
//             }
//         }
        
//         private void loadTopWords(String fileName) {
//             try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
//                 String line;
//                 while ((line = reader.readLine()) != null) {
//                     String[] parts = line.split("\\t");
//                     if (parts.length > 0) {
//                         topWords.add(parts[0]);
//                     }
//                 }
//             } catch (IOException e) {
//                 System.err.println("Error reading top words file: " + StringUtils.stringifyException(e));
//             }
//         }
        
//         @Override
//         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//             String line = value.toString().toLowerCase();
//             String[] tokens = line.split("[^\\w']+");
            
//             // Local aggregation at function level
//             Map<WordPair, Integer> localCounts = new HashMap<>();
            
//             for (int i = 0; i < tokens.length; i++) {
//                 if (!topWords.contains(tokens[i])) {
//                     continue;
//                 }
                
//                 // Check for co-occurring words within distance
//                 for (int j = Math.max(0, i - distance); j < i; j++) {
//                     if (topWords.contains(tokens[j])) {
//                         word1.set(tokens[i]);
//                         word2.set(tokens[j]);
//                         WordPair pair = new WordPair(new Text(word1), new Text(word2));
//                         localCounts.put(pair, localCounts.getOrDefault(pair, 0) + 1);
//                     }
//                 }
                
//                 for (int j = i + 1; j <= Math.min(tokens.length - 1, i + distance); j++) {
//                     if (topWords.contains(tokens[j])) {
//                         word1.set(tokens[i]);
//                         word2.set(tokens[j]);
//                         WordPair pair = new WordPair(new Text(word1), new Text(word2));
//                         localCounts.put(pair, localCounts.getOrDefault(pair, 0) + 1);
//                     }
//                 }
//             }
            
//             // Emit aggregated counts at the end of processing this input split
//             for (Map.Entry<WordPair, Integer> entry : localCounts.entrySet()) {
//                 context.write(entry.getKey(), new IntWritable(entry.getValue()));
//             }
//         }
//     }
    
//     public static class CoReducer extends Reducer<WordPair, IntWritable, WordPair, IntWritable> {
//         @Override
//         public void reduce(WordPair key, Iterable<IntWritable> values, Context context) 
//                 throws IOException, InterruptedException {
//             int sum = 0;
//             for (IntWritable val : values) {
//                 sum += val.get();
//             }
//             context.write(key, new IntWritable(sum));
//         }
//     }
    
//     public static void main(String[] args) throws Exception {
//         // Get aggregation type from command line
//         boolean useClassLevelAggregation = args.length > 4 && "class".equals(args[4]);
        
//         Configuration conf = new Configuration();
//         Job job = Job.getInstance(conf, "Co-occurring Matrix with Local Aggregation");
        
//         job.setJarByClass(Question4e.class);
        
//         // Set mapper based on aggregation type
//         if (useClassLevelAggregation) {
//             job.setMapperClass(ClassLevelAggregationMapper.class);
//         } else {
//             job.setMapperClass(FunctionLevelAggregationMapper.class);
//         }
        
//         job.setReducerClass(CoReducer.class);
        
//         job.setOutputKeyClass(WordPair.class);
//         job.setOutputValueClass(IntWritable.class);
//         job.setMapOutputKeyClass(WordPair.class);
//         job.setMapOutputValueClass(IntWritable.class);
        
//         // Set distance parameter
//         job.getConfiguration().setInt("distance", Integer.parseInt(args[3]));
        
//         // Add the top words file to distributed cache
//         job.addCacheFile(new Path(args[2]).toUri());
        
//         FileInputFormat.addInputPath(job, new Path(args[0]));
//         FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
//         System.exit(job.waitForCompletion(true) ? 0 : 1);
//     }
// }




import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class Question4d {
    
    // Same WordPair class as in Question4b
    static class WordPair implements WritableComparable<WordPair> {
        private Text word1;
        private Text word2;

        public WordPair() {
            this.word1 = new Text();
            this.word2 = new Text();
        }

        public WordPair(Text word1, Text word2) {
            this.word1 = word1;
            this.word2 = word2;
        }

        public Text getWord1() {
            return word1;
        }

        public Text getWord2() {
            return word2;
        }
        
        @Override
        public int compareTo(WordPair o) {
            String thisValue = this.word1.toString() + " " + this.word2.toString();
            String thatValue = o.word1.toString() + " " + o.word2.toString();
            return thisValue.compareTo(thatValue);
        }
        
        @Override
        public void write(java.io.DataOutput out) throws IOException {
            word1.write(out);
            word2.write(out);
        }

        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            word1.readFields(in);
            word2.readFields(in);
        }

        @Override
        public String toString() {
            return "{" + word1.toString() + "," + word2.toString() + "}";
        }
        
        @Override
        public int hashCode() {
            return word1.hashCode() * 163 + word2.hashCode();
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof WordPair) {
                WordPair other = (WordPair) obj;
                return word1.equals(other.word1) && word2.equals(other.word2);
            }
            return false;
        }
    }
    
    /**
     * PAIRS APPROACH
     * Mapper with class-level aggregation 
     * This approach maintains state within the mapper instance
     */
    public static class PairsClassLevelAggregationMapper extends Mapper<Object, Text, WordPair, IntWritable> {
        private Set<String> topWords = new HashSet<>();
        private Text word1 = new Text();
        private Text word2 = new Text();
        private int distance;
        
        // Map to store aggregated counts within this mapper instance
        private Map<WordPair, Integer> pairCounts = new HashMap<>();
        
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
            
            for (int i = 0; i < tokens.length; i++) {
                if (!topWords.contains(tokens[i])) {
                    continue;
                }
                
                // Check for co-occurring words within distance
                for (int j = Math.max(0, i - distance); j < i; j++) {
                    if (topWords.contains(tokens[j])) {
                        word1.set(tokens[i]);
                        word2.set(tokens[j]);
                        WordPair pair = new WordPair(new Text(word1), new Text(word2));
                        pairCounts.put(pair, pairCounts.getOrDefault(pair, 0) + 1);
                    }
                }
                
                for (int j = i + 1; j <= Math.min(tokens.length - 1, i + distance); j++) {
                    if (topWords.contains(tokens[j])) {
                        word1.set(tokens[i]);
                        word2.set(tokens[j]);
                        WordPair pair = new WordPair(new Text(word1), new Text(word2));
                        pairCounts.put(pair, pairCounts.getOrDefault(pair, 0) + 1);
                    }
                }
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit aggregated counts at the end of the mapper's lifecycle
            for (Map.Entry<WordPair, Integer> entry : pairCounts.entrySet()) {
                context.write(entry.getKey(), new IntWritable(entry.getValue()));
            }
        }
    }
    
    /**
     * PAIRS APPROACH
     * Mapper with function-level aggregation
     * This approach aggregates counts within each map function call
     */
    public static class PairsFunctionLevelAggregationMapper extends Mapper<Object, Text, WordPair, IntWritable> {
        private Set<String> topWords = new HashSet<>();
        private Text word1 = new Text();
        private Text word2 = new Text();
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
            
            // Local aggregation at function level
            Map<WordPair, Integer> localCounts = new HashMap<>();
            
            for (int i = 0; i < tokens.length; i++) {
                if (!topWords.contains(tokens[i])) {
                    continue;
                }
                
                // Check for co-occurring words within distance
                for (int j = Math.max(0, i - distance); j < i; j++) {
                    if (topWords.contains(tokens[j])) {
                        word1.set(tokens[i]);
                        word2.set(tokens[j]);
                        WordPair pair = new WordPair(new Text(word1), new Text(word2));
                        localCounts.put(pair, localCounts.getOrDefault(pair, 0) + 1);
                    }
                }
                
                for (int j = i + 1; j <= Math.min(tokens.length - 1, i + distance); j++) {
                    if (topWords.contains(tokens[j])) {
                        word1.set(tokens[i]);
                        word2.set(tokens[j]);
                        WordPair pair = new WordPair(new Text(word1), new Text(word2));
                        localCounts.put(pair, localCounts.getOrDefault(pair, 0) + 1);
                    }
                }
            }
            
            // Emit aggregated counts at the end of processing this input split
            for (Map.Entry<WordPair, Integer> entry : localCounts.entrySet()) {
                context.write(entry.getKey(), new IntWritable(entry.getValue()));
            }
        }
    }
    
    /**
     * STRIPES APPROACH
     * Mapper with class-level aggregation using the stripes approach
     */
    public static class StripesClassLevelAggregationMapper extends Mapper<Object, Text, Text, MapWritable> {
        private Set<String> topWords = new HashSet<>();
        private int distance;
        
        // Map to store aggregated stripes within this mapper instance
        // Key: Word, Value: Map of co-occurring words and their counts
        private Map<String, Map<String, Integer>> wordStripes = new HashMap<>();
        
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
            
            for (int i = 0; i < tokens.length; i++) {
                String token = tokens[i];
                if (!topWords.contains(token)) {
                    continue;
                }
                
                // If this word is not yet in the stripes map, add it
                if (!wordStripes.containsKey(token)) {
                    wordStripes.put(token, new HashMap<>());
                }
                
                Map<String, Integer> stripes = wordStripes.get(token);
                
                // Look backward within distance
                for (int j = Math.max(0, i - distance); j < i; j++) {
                    if (topWords.contains(tokens[j])) {
                        String coWord = tokens[j];
                        stripes.put(coWord, stripes.getOrDefault(coWord, 0) + 1);
                    }
                }
                
                // Look forward within distance
                for (int j = i + 1; j <= Math.min(tokens.length - 1, i + distance); j++) {
                    if (topWords.contains(tokens[j])) {
                        String coWord = tokens[j];
                        stripes.put(coWord, stripes.getOrDefault(coWord, 0) + 1);
                    }
                }
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Convert Java maps to Hadoop's MapWritable and emit
            for (Map.Entry<String, Map<String, Integer>> entry : wordStripes.entrySet()) {
                Text wordKey = new Text(entry.getKey());
                MapWritable stripe = new MapWritable();
                
                for (Map.Entry<String, Integer> coWordEntry : entry.getValue().entrySet()) {
                    stripe.put(new Text(coWordEntry.getKey()), new IntWritable(coWordEntry.getValue()));
                }
                
                context.write(wordKey, stripe);
            }
        }
    }
    
    /**
     * STRIPES APPROACH
     * Mapper with function-level aggregation using the stripes approach
     */
    public static class StripesFunctionLevelAggregationMapper extends Mapper<Object, Text, Text, MapWritable> {
        private Set<String> topWords = new HashSet<>();
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
            
            // Local aggregation at function level
            Map<String, Map<String, Integer>> localWordStripes = new HashMap<>();
            
            for (int i = 0; i < tokens.length; i++) {
                String token = tokens[i];
                if (!topWords.contains(token)) {
                    continue;
                }
                
                // If this word is not yet in the local stripes map, add it
                if (!localWordStripes.containsKey(token)) {
                    localWordStripes.put(token, new HashMap<>());
                }
                
                Map<String, Integer> stripes = localWordStripes.get(token);
                
                // Look backward within distance
                for (int j = Math.max(0, i - distance); j < i; j++) {
                    if (topWords.contains(tokens[j])) {
                        String coWord = tokens[j];
                        stripes.put(coWord, stripes.getOrDefault(coWord, 0) + 1);
                    }
                }
                
                // Look forward within distance
                for (int j = i + 1; j <= Math.min(tokens.length - 1, i + distance); j++) {
                    if (topWords.contains(tokens[j])) {
                        String coWord = tokens[j];
                        stripes.put(coWord, stripes.getOrDefault(coWord, 0) + 1);
                    }
                }
            }
            
            // Convert Java maps to Hadoop's MapWritable and emit
            for (Map.Entry<String, Map<String, Integer>> entry : localWordStripes.entrySet()) {
                Text wordKey = new Text(entry.getKey());
                MapWritable stripe = new MapWritable();
                
                for (Map.Entry<String, Integer> coWordEntry : entry.getValue().entrySet()) {
                    stripe.put(new Text(coWordEntry.getKey()), new IntWritable(coWordEntry.getValue()));
                }
                
                context.write(wordKey, stripe);
            }
        }
    }
    
    /**
     * Reducer for the Pairs approach
     */
    public static class PairsReducer extends Reducer<WordPair, IntWritable, WordPair, IntWritable> {
        @Override
        public void reduce(WordPair key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    
    /**
     * Reducer for the Stripes approach
     */
    public static class StripesReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            // Create a map to aggregate counts
            MapWritable result = new MapWritable();
            
            // Combine all stripes for the key word
            for (MapWritable value : values) {
                for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
                    Text coWord = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();
                    
                    // If co-word already exists in result, add to its count
                    if (result.containsKey(coWord)) {
                        IntWritable existingCount = (IntWritable) result.get(coWord);
                        result.put(coWord, new IntWritable(existingCount.get() + count.get()));
                    } else {
                        // Otherwise add it to the result
                        result.put(coWord, count);
                    }
                }
            }
            
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: Question4e <input> <output> <topWords> <distance> <approachType> <aggregationType>");
            System.err.println("approachType: pairs or stripes");
            System.err.println("aggregationType: class or function");
            System.exit(1);
        }
        
        // Get approach and aggregation type from command line
        String approachType = args[4];
        String aggregationType = args.length > 5 ? args[5] : "function";
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Co-occurring Matrix with " + approachType + " approach and " + 
                aggregationType + " level aggregation");
        
        job.setJarByClass(Question4d.class);
        
        // Set distance parameter
        job.getConfiguration().setInt("distance", Integer.parseInt(args[3]));
        
        // Add the top words file to distributed cache
        job.addCacheFile(new Path(args[2]).toUri());
        
        // Set mapper, reducer, and output types based on approach
        if ("pairs".equalsIgnoreCase(approachType)) {
            // Pairs approach configuration
            if ("class".equalsIgnoreCase(aggregationType)) {
                job.setMapperClass(PairsClassLevelAggregationMapper.class);
            } else {
                job.setMapperClass(PairsFunctionLevelAggregationMapper.class);
            }
            
            job.setReducerClass(PairsReducer.class);
            job.setOutputKeyClass(WordPair.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapOutputKeyClass(WordPair.class);
            job.setMapOutputValueClass(IntWritable.class);
        } else if ("stripes".equalsIgnoreCase(approachType)) {
            // Stripes approach configuration
            if ("class".equalsIgnoreCase(aggregationType)) {
                job.setMapperClass(StripesClassLevelAggregationMapper.class);
            } else {
                job.setMapperClass(StripesFunctionLevelAggregationMapper.class);
            }
            
            job.setReducerClass(StripesReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MapWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(MapWritable.class);
        } else {
            System.err.println("Invalid approach type. Please use 'pairs' or 'stripes'.");
            System.exit(1);
        }
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
