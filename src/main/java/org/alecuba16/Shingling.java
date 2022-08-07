package org.alecuba16;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Job 1
// Mapper:
// 1. Read each line document
// 2. Generate shingling (ngrams) dict
// Reducer:
// 1. Collect shingling

public class Shingling {

    private Shingling() {
        throw new IllegalStateException("Shingling class");
    }

    public static class ShinglingMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String substr;
            String line = value.toString();
            int shinglingLength = context.getConfiguration().getInt("shinglingLength",2);
            for (int i = 0; i < line.length(); i ++) {
                substr = value.toString().substring(i, Math.min(line.length(), i + shinglingLength));
                if (substr.length()<shinglingLength) substr = substr + " ";
                context.write(new Text(substr), new Text(key.toString()));
            }
        }
    }

    public static class ShinglingCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> rows = new ArrayList<>();
            for (Text value : values) {
                rows.add(value.toString());
            }
            if (!rows.isEmpty()) context.write(key, new Text(String.join(",", rows)));
        }
    }

    public static class ShinglingReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Initialize the count of the ads that have been published.
            List<String> rows = new ArrayList<>();
            for (Text value : values) {
                rows.add(value.toString());
            }
            context.write(new Text("\""+key.toString()+"\""), new Text("\""+String.join("\",\"", rows)+"\""));
        }

        // This override of the setup method is for including to the output file a header like a regular CSV
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // Get the value if write header is enabled
            Text column = new Text("Shingling");
            Text values = new Text("rowsOffset");
            context.write(column, values);
        }
    }

    public static void configureJob(Job job, String pathIn, String pathOut, int shinglingLength) throws IOException {
        job.setJarByClass(Shingling.class);

        job.getConfiguration().setInt("shinglingLength", shinglingLength);
        job.setMapperClass(Shingling.ShinglingMapper.class);
        job.setCombinerClass(Shingling.ShinglingCombiner.class);
        job.setReducerClass(Shingling.ShinglingReducer.class);

        // Set the mapper class it must use
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        // The output will be Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // This is for having a csv like output, instead space-separated key-values
        job.getConfiguration().set("mapred.textoutputformat.separator", ",");

        // Cleanup output path
        Path outputPath = new Path(pathOut);
        FileSystem fs = FileSystem.get(outputPath.toUri(), job.getConfiguration());
        fs.delete(outputPath, true);

        // The files the job will read from/write to
        FileInputFormat.addInputPath(job, new Path(pathIn));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
    }

}
