import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class TFIDFDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: TFIDFDriver <input path> <output path> <top 100 terms> <stopwords file>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TF-IDF Calculation");

        job.setJarByClass(TFIDFDriver.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);  

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);

        // Prevent errors if output path already exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        // Add top 100 words and stopwords to distributed cache
        job.addCacheFile(new Path(args[2]).toUri()); // top_100_words.txt
        job.addCacheFile(new Path(args[3]).toUri()); // stopwords.txt

        // Add OpenNLP JAR to classpath
        job.addFileToClassPath(new Path("/libs/opennlp-tools-1.9.3.jar"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

