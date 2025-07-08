import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import opennlp.tools.stemmer.PorterStemmer;

import java.io.*;
import java.util.HashSet;
import java.util.StringTokenizer;

public class DFMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text term = new Text();
    private Text docID = new Text();
    private HashSet<String> stopwords = new HashSet<>();
    private PorterStemmer stemmer = new PorterStemmer();
    private String documentID; // Store filename as document ID

    @Override
    protected void setup(Context context) throws IOException {
        // Read stopwords from distributed cache or HDFS
        Configuration conf = context.getConfiguration();
        Path stopwordPath = new Path(conf.get("stopwords.file"));

        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stopwordPath)));
        String line;
        while ((line = br.readLine()) != null) {
            stopwords.add(line.trim().toLowerCase());
        }
        br.close();

        // Extract document ID from file path (without extension)
        String inputPath = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
        int dotIndex = inputPath.lastIndexOf('.'); // Find last dot (.)
        if (dotIndex > 0) {
            documentID = inputPath.substring(0, dotIndex); // Remove extension
        } else {
            documentID = inputPath; // No extension found, keep full name
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Process the text file content
        String text = value.toString().toLowerCase().replaceAll("[^a-zA-Z\\s]", " "); // Remove numbers & special chars
        HashSet<String> uniqueTerms = new HashSet<>();

        StringTokenizer tokenizer = new StringTokenizer(text);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (!stopwords.contains(token)) {
                String stemmed = stemmer.stem(token);
                if (!stemmed.isEmpty()) {
                    uniqueTerms.add(stemmed);
                }
            }
        }

        // Emit (TERM, DOC_ID) for each unique term in the document
        for (String uniqueTerm : uniqueTerms) {
            term.set(uniqueTerm);
            docID.set(documentID);
            context.write(term, docID);
        }
    }
}

