import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class DFReducer extends Reducer<Text, Text, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> uniqueDocs = new HashSet<>();

        for (Text docID : values) {
            uniqueDocs.add(docID.toString());  // Collect unique document IDs
        }

        result.set(uniqueDocs.size());  // Count of unique documents
        context.write(key, result);  // Output TERM <TAB> DF
    }
}

