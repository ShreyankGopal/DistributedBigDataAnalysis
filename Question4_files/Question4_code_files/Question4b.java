
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

class WordPair implements WritableComparable<WordPair> {
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

    public Text getword1() {
        return word1;
    }

    public Text getWord2() {
        return word2;
    }
    @Override
    public int compareTo(WordPair o) {
        String thisValue=this.word1.toString()+" "+this.word2.toString();
        String thatValue=o.word1.toString()+" "+o.word2.toString();
        return thisValue.compareTo(thatValue);
    }
    @Override
    public void write(DataOutput out) throws IOException {
        word1.write(out);
        word2.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word1.readFields(in);
        word2.readFields(in);
    }

    @Override
    public String toString() {
        return "{" + word1.toString() + "," + word2.toString() + "}";
    }
}
public class Question4b {
        
	public static class pairMapper extends Mapper<Object, Text, WordPair, IntWritable> {
        private  Set<String> sett = new HashSet<>();
        private final static IntWritable one = new IntWritable(1);
		private Text word1 = new Text();
		private Text word2 = new Text();
        private int d;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            d = conf.getInt("distance", 0);
            URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI uri : cacheFiles) {
                    top50(new Path(uri.getPath()).getName());
                }
            }
        }
        private void top50(String fileName) {//setup needs to be in tokenise mapper class.
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String []parts=line.split("\\t");
                    if(parts.length!=0){
                        sett.add(parts[0]);
                    }
                    
                }
            } catch (IOException e) {
                System.err.println("Error reading stop words file: " + StringUtils.stringifyException(e));
            }
        }
		
		
		//private boolean caseSensitive = false;
		//private Set<String> patternsToSkip = new HashSet<String>();
        
        
		

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().toLowerCase();
		
			// Split on tab and extract only the first word
			
			
				
			String[] tokens = line.split("[^\\w']+");
		
				for (int i = 0; i < tokens.length ; i++) {
                    boolean exists1 = sett.contains(tokens[i]);
                    boolean exists2 = (i + d < tokens.length) && sett.contains(tokens[i + d]);
                    boolean exists3 = (i - d >= 0) && sett.contains(tokens[i - d]);

                    if (exists1 && exists2) {
                        word1.set(tokens[i]);
                        word2.set(tokens[i + d]);
                        context.write(new WordPair(word1, word2), one);
                    }
                    if (exists1 && exists3) {
                        word1.set(tokens[i]);
                        word2.set(tokens[i - d]);
                        context.write(new WordPair(word1, word2), one);
                    }
					
					
				}
			
		}
	}

	public static class CoReducer extends Reducer<WordPair, IntWritable, WordPair, IntWritable> {
    //private PriorityQueue<WordPairCount> topPairs = new PriorityQueue<>(Comparator.comparingInt(wp -> wp.count));//we us the priority queue data structure to get the top 50;

    private Map<String, Map<String, Integer>> coMatrix = new HashMap<>();

    @Override
    public void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        context.write(key,new IntWritable(sum));

        // Update co-occurrence matrix
        // coMatrix.putIfAbsent(w1, new HashMap<>());
        // coMatrix.get(w1).put(w2, coMatrix.get(w1).getOrDefault(w2, 0) + sum);

    }

    // @Override
    // protected void cleanup(Context context) throws IOException, InterruptedException {
    //     // Write matrix rows as output
    //     for (Map.Entry<String, Map<String, Integer>> entry : coMatrix.entrySet()) {
    //         String word = entry.getKey();
    //         StringBuilder sb = new StringBuilder();

    //         for (Map.Entry<String, Integer> coEntry : entry.getValue().entrySet()) {
    //             sb.append(coEntry.getKey()).append(":").append(coEntry.getValue()).append(" ");
    //         }

    //         // Output format: "word \t w2:count w3:count ..."
    //         context.write(new Text(word), new Text(sb.toString().trim()));
    //     }
    // }

    

    // Helper class to store word pairs and their frequency count
    
    }

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top 50 coccurences");

		job.setMapperClass(pairMapper.class);
		// job.setCombinerClass(IntSumReducer.class); // enable to use 'local aggregation'
		job.setReducerClass(CoReducer.class);
        job.getConfiguration().setInt("distance", Integer.parseInt(args[3]));
        job.addCacheFile(new Path(args[2]).toUri());
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(IntWritable.class);

		
        job.setMapOutputKeyClass(WordPair.class);
        job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}