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

import java.io.*;
import java.util.*;

public class CollectCandidates {
    public static class CollectCandidatesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.contains("Hash")) {
                String[] lineColumns = line.split("\",\"");
                String[] candidatesLines = lineColumns[1].substring(0,lineColumns[1].length()-1).split("\\|");
                for (String candidateLine : candidatesLines) {
                    String[] others = Arrays.stream(candidatesLines).filter(x -> !Objects.equals(x, candidateLine)).toArray(String[]::new);
                    context.write(new Text(candidateLine), new Text(String.join("|", others)));
                }
            }
        }
    }

    public static class CollectCandidatesReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Initialize the count of the ads that have been published.
            List<String> pairs = new ArrayList<>();
            String[] tmp;
            for (Text value : values) {
                if(!value.toString().equals("")) {
                    tmp = value.toString().split("\\|");
                    Collections.addAll(pairs,tmp);
                }
            }

            String[] uniqueValues=pairs.stream().distinct().toArray(String[]::new);
            if (uniqueValues.length > 0) {
                for (String uniqueValue : uniqueValues) {
                    context.write(key, new Text(uniqueValue));
                }
            }
        }

        // This override of the setup method is for including to the output file a header like a regular CSV
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // Get the value if write header is enabled
            Text column = new Text("Text1");
            Text values = new Text("Text2");
            context.write(column, values);
        }
    }


    public static void configureJob(Job job, String documentPath, String candidatesPath, String pathOut) throws IOException {
        job.setJarByClass(CollectCandidates.class);

        job.setMapperClass(CollectCandidates.CollectCandidatesMapper.class);
        job.setReducerClass(CollectCandidates.CollectCandidatesReducer.class);

        // Set the mapper class it must use
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        // The output will be Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().setStrings("documentPath", documentPath);

        // This is for having a csv like output, instead space-separated key-values
        job.getConfiguration().set("mapred.textoutputformat.separator", ",");

        // Cleanup output path
        Path outputPath = new Path(pathOut);
        FileSystem fs = FileSystem.get(outputPath.toUri(), job.getConfiguration());
        fs.delete(outputPath, true);

        // The files the job will read from/write to
        FileInputFormat.addInputPath(job, new Path(candidatesPath));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
    }
}
