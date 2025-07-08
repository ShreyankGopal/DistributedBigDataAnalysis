import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import opennlp.tools.stemmer.PorterStemmer;
import java.net.URI;
import java.io.*;
import java.util.*;

public class TFIDFMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private HashSet<String> stopwords = new HashSet<>();
    private TreeSet<String> topTerms = new TreeSet<>();
    private PorterStemmer stemmer = new PorterStemmer();
    private String documentID;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles == null || cacheFiles.length < 2) {
            throw new IOException("Expected top_100_words.txt and stopwords.txt in the distributed cache.");
        }

        for (URI uri : cacheFiles) {
            Path filePath = new Path(uri);
            String fileName = filePath.getName();

            // Load top 100 words
            if (fileName.equals("top_100_words.txt")) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length >= 1) {
                        topTerms.add(parts[0].trim().toLowerCase());
                    }
                }
                reader.close();
            } 
            // Load stopwords
            else if (fileName.equals("stopwords.txt")) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
                String line;
                while ((line = reader.readLine()) != null) {
                    stopwords.add(line.trim().toLowerCase());
                }
                reader.close();
            }
        }

        // Extract document ID from file name
        String inputPath = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
        int dotIndex = inputPath.lastIndexOf('.');
        documentID = (dotIndex > 0) ? inputPath.substring(0, dotIndex) : inputPath;
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString().toLowerCase().replaceAll("[^a-zA-Z\\s]", " ");
        StringTokenizer tokenizer = new StringTokenizer(text);
        HashMap<String, Integer> termCounts = new HashMap<>();

        // Tokenize, remove stopwords, apply stemming, and count occurrences
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (!stopwords.contains(token)) {
                String stemmed = stemmer.stem(token);
                if (topTerms.contains(stemmed)) {
                    termCounts.put(stemmed, termCounts.getOrDefault(stemmed, 0) + 1);
                }
            }
        }

        // Convert termCounts to a MapWritable stripe (sorted order)
        MapWritable stripe = new MapWritable();
	for (String term : topTerms) {  // Now this follows alphabetical order
	    stripe.put(new Text(term), new IntWritable(termCounts.getOrDefault(term, 0)));
	}

        // Emit documentID as key, and term frequency stripe as value
        context.write(new Text(documentID), stripe);
    }
}

