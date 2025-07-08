import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DFDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: DFDriver <input> <output> <stopwords>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        String stopwordsPath = args[2];

        Configuration conf = new Configuration();
        conf.set("stopwords.file", stopwordsPath);

        Job job = Job.getInstance(conf, "Document Frequency");
        job.setJarByClass(DFDriver.class);
        job.setMapperClass(DFMapper.class);
        job.setReducerClass(DFReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.addFileToClassPath(new Path("/libs/opennlp-tools-1.9.3.jar"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

