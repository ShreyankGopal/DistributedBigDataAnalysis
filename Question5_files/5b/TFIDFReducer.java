import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.*;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import java.io.File;

public class TFIDFReducer extends Reducer<Text, MapWritable, Text, Text> {
    private int totalDocuments;
    private HashMap<String, Integer> documentFrequencies = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        totalDocuments = conf.getInt("total.documents", 10000); // Default value

        // Get cached files
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            // Convert URI to local path
            Path cachedFilePath = new Path(cacheFiles[0].getPath()); // Get actual file path
            File cachedFile = new File(cachedFilePath.getName());    // Localized file name

            // Read from the cached file
            Scanner scanner = new Scanner(cachedFile);
            while (scanner.hasNextLine()) {
                String[] parts = scanner.nextLine().split("\t");
                if (parts.length == 2) {
                    documentFrequencies.put(parts[0], Integer.parseInt(parts[1]));
                }
            }
            scanner.close();
        }
    }

    @Override
    public void reduce(Text docID, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> termCounts = new HashMap<>();

        // Initialize all terms from top_100_words.txt with 0
        for (String term : documentFrequencies.keySet()) {
            termCounts.put(term, 0);
        }

        // Aggregate term frequencies from all stripes
        for (MapWritable stripe : values) {
            for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                String term = entry.getKey().toString();
                int tf = ((IntWritable) entry.getValue()).get();
                termCounts.put(term, termCounts.getOrDefault(term, 0) + tf);
            }
        }

        // Compute TF-IDF for each term in top 100 words, maintaining order
        TreeMap<String, Double> sortedTFIDF = new TreeMap<>(); // Ensures alphabetical order
        for (Map.Entry<String, Integer> entry : termCounts.entrySet()) {
            String term = entry.getKey();
            int tf = entry.getValue();
            int df = documentFrequencies.getOrDefault(term, 1); // Avoid division by zero

            // TF-IDF formula: TF * log10(N / (DF + 1))
            double tfidf = tf * Math.log10((double) totalDocuments / (df + 1));

            sortedTFIDF.put(term, tfidf);
        }

        // Emit in sorted order
        for (Map.Entry<String, Double> entry : sortedTFIDF.entrySet()) {
            context.write(new Text(docID.toString()), new Text(entry.getKey() + "\t" + String.format("%.6f", entry.getValue())));
        }
    }
}

