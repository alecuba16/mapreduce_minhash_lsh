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
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

// Job 1
// Distribute the shingling dict and randomized arrays to all nodes
// Each node mapper:
// 1. Read a line of the document
// 2. Generate the shingling of the line
// 3. Generate the one hot encoding
// 4. Calculate the signature of the line
// 5. Generate the Hash for the N bands (the key)
// 6. Write that key and the sentence row id as the value.
// Reducer / Combiner:
// 1. Collect distinct sentence row id for each key
// 2. Generate csv with key column and key pairs
public class MinHashLSH {

    private MinHashLSH() {
        throw new IllegalStateException("MinHashLSH class");
    }

    public static class MinHashLSHMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int numBands = 10;
        private int signatureSize = 50;
        private int shinglingSize = 3;
        private HashMap<String, String> shinglingMap;

        @Override
        public void setup(Context context) throws IOException {
            numBands = context.getConfiguration().getInt("numBands", 1);
            shinglingSize = context.getConfiguration().getInt("shinglingSize", 50);
            signatureSize = context.getConfiguration().getInt("signatureSize", 50);
            // Get the column names
            shinglingMap = new HashMap<>();
            URI[] shinglingMapFiles = context.getCacheFiles();
            if (shinglingMapFiles != null && shinglingMapFiles.length > 0) {
                for (URI shinglingMapFile : shinglingMapFiles) {
                    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(shinglingMapFile.getPath()))) {
                        String line;
                        while ((line = bufferedReader.readLine()) != null) {
                            if (line.contains("Shingling")) continue;
                            line = line.substring(1, line.length() - 1);
                            String[] shinglingArray = line.split("\",\"");
                            String shingling = shinglingArray[0];
                            String rows = shinglingArray[1];
                            shinglingMap.put(shingling, rows);
                        }
                    }
                }
            }
        }

        private List<Boolean> generateOneHot(String line) {
            String substr;
            List<Boolean> oneHot = new ArrayList<>();
            for (int i = 0; i < line.length(); i += shinglingSize) {
                substr = line.substring(i, Math.min(line.length(), i + shinglingSize));
                if (substr.length() < shinglingSize) substr = substr + " ";
                for (String shingling : shinglingMap.keySet()) {
                    oneHot.add(substr.equals(shingling));
                }
            }
            return oneHot;
        }

        private int[] getSignature(List<Boolean> oneHot, String[] permutationsBase64) {
            int tempSigPointer;
            int[] signature = new int[signatureSize];
            int signaturePos = 0;
            for (String permutationBase64 : permutationsBase64) {
                int[] permutations = convertFromBase64(permutationBase64);
                List<Integer> permutationsL = new ArrayList<>();
                Collections.addAll(permutationsL, Arrays.stream(permutations).boxed().toArray(Integer[]::new));
                int i = 0;
                boolean found = false;
                while (!found && (i < permutationsL.size())) {
                    tempSigPointer = permutationsL.indexOf(i);
                    if (Boolean.TRUE.equals(oneHot.get(tempSigPointer))) {
                        found = true;
                    } else {
                        i++;
                    }
                }
                signature[signaturePos] = i;
                signaturePos++;
            }
            return signature;
        }

        private int[][] generateSignatureBands(int[] signature) {
            int bandSize = signature.length / numBands;
            int[][] signatureBands = new int[numBands][bandSize];
            for (int i = 0; i < signature.length; i++) {
                signatureBands[(i / bandSize)][i % bandSize] = signature[i];
            }
            return signatureBands;
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            List<Boolean> oneHot = generateOneHot(line);
            // Get the signatures
            String[] permutationsBase64 = context.getConfiguration().getStrings("permutationsBase64");
            int[] signature = getSignature(oneHot, permutationsBase64);

            List<String> signatureL = new ArrayList<>();
            for (int signatureElement : signature) {
                signatureL.add(Integer.toString(signatureElement));
            }

            String signatureStr = String.join(",", signatureL.toArray(new String[0]));

            int[][] signatureBands = generateSignatureBands(signature);

            String outputVal = key.get() + "-" + signatureStr + "-" + line;
            for (int[] signatureBand : signatureBands) {
                String signatureBandStr = IntStream.of(signatureBand)
                        .mapToObj(Integer::toString)
                        .collect(Collectors.joining(","));
                context.write(new Text(signatureBandStr), new Text(outputVal));
            }
        }
    }

    public static class MinHashLSHReducer extends Reducer<Text, Text, Text, Text> {
        private double jaccardThreshold = 0.5;

        private double jaccard(int[] e1, int[] e2) {
            Set<Integer> e1Set = new HashSet<>();
            for (int e1Element : e1) {
                e1Set.add(e1Element);
            }
            Set<Integer> e2Set = new HashSet<>();
            for (int e2Element : e2) {
                e2Set.add(e2Element);
            }
            Set<Integer> intersection = new HashSet<>(e1Set);
            intersection.retainAll(e2Set);
            Set<Integer> union = new HashSet<>(e1Set);
            union.addAll(e2Set);

            // Jaccard  = A^B / (AUB)
            return (double) intersection.size() / (double) (union.size());
        }

        private List<List<String>> filterCandidatesPairs(List<String> buckedRowsOffsets, List<String> buckedLines, List<String> buckedSignatures) {
            int[] signature1;
            int[] signature2;
            List<String> candidatePairsRowsOffsets = new ArrayList<>();
            List<String> candidatePairsLines = new ArrayList<>();
            for (int i = 0; i < buckedSignatures.size() - 1; i++) {
                signature1 = Arrays.stream(buckedSignatures.get(i).split(",")).mapToInt(Integer::parseInt).toArray();
                for (int j = i + 1; j < buckedSignatures.size(); j++) {
                    signature2 = Arrays.stream(buckedSignatures.get(i).split(",")).mapToInt(Integer::parseInt).toArray();
                    if (jaccard(signature1, signature2) > jaccardThreshold) {
                        if (!candidatePairsRowsOffsets.contains(buckedRowsOffsets.get(i))) candidatePairsRowsOffsets.add(buckedRowsOffsets.get(i));

                        if (!candidatePairsRowsOffsets.contains(buckedRowsOffsets.get(j))) candidatePairsRowsOffsets.add(buckedRowsOffsets.get(j));

                        if (!candidatePairsLines.contains(buckedLines.get(i))) candidatePairsLines.add(buckedLines.get(i));

                        if (!candidatePairsLines.contains(buckedLines.get(j))) candidatePairsLines.add(buckedLines.get(j));

                    }
                }
            }
            List<List<String>> candidatePairs = new ArrayList<>();
            candidatePairs.add(candidatePairsRowsOffsets);
            candidatePairs.add(candidatePairsLines);
            return candidatePairs;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Initialize the count of the ads that have been published.
            List<List<String>> candidatesPairs;
            List<String> candidatesRowsOffsets = new ArrayList<>();
            List<String> candidatesSignature = new ArrayList<>();
            List<String> candidatesLines = new ArrayList<>();

            for (Text row : values) {
                String[] rowSplit = row.toString().split("-");
                candidatesRowsOffsets.add(rowSplit[0]);
                candidatesSignature.add(rowSplit[1]);
                candidatesLines.add(rowSplit[2]);
            }

            if(candidatesRowsOffsets.size() > 1) {
                candidatesPairs = filterCandidatesPairs(candidatesRowsOffsets,candidatesLines, candidatesSignature);
                if (!candidatesPairs.isEmpty()) {
                    List<String> lines=candidatesPairs.get(1);
                    String finalStr = "\"" + String.join("|", lines) + "\"";
                    context.write(new Text("\"" + key.toString() + "\""), new Text(finalStr));
                }
            }
        }

        // This override of the setup method is for including to the output file a header like a regular CSV
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // Get the value if write header is enabled
            Text column = new Text("Hash");
            Text values = new Text("candidatesRowsOffsets");

            jaccardThreshold = context.getConfiguration().getDouble("jaccardThreshold", 0.5);

            context.write(column, values);
        }
    }

    public static String convertToBase64(int[] ints) {
        ByteBuffer buf = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE * ints.length);
        buf.asIntBuffer().put(ints);
        return Base64.getEncoder().encodeToString(buf.array());
    }

    public static int[] convertFromBase64(String base64str) {
        byte[] bytes = Base64.getDecoder().decode(base64str);
        IntBuffer buf = ByteBuffer.wrap(bytes).asIntBuffer();
        int[] ints = new int[buf.limit()];
        buf.get(ints);
        return ints;
    }


    public static String[] generatePermutationsArrays(String shingling, int signatureSize) throws IOException {
        // numLines for the signature

        long numShinglingElements = Files.lines(Paths.get(shingling + "part-r-00000"), Charset.defaultCharset()).count();
        String[] permutations = new String[signatureSize];

        for (int s = 0; s < signatureSize; s++) {
            int[] permutation = IntStream.rangeClosed(0, (int) numShinglingElements - 1).toArray();
            Random rand = new Random();
            for (int i = 0; i < permutation.length; i++) {
                int randomIndexToSwap = rand.nextInt(permutation.length);
                int temp = permutation[randomIndexToSwap];
                permutation[randomIndexToSwap] = permutation[i];
                permutation[i] = temp;
            }
            permutations[s] = convertToBase64(permutation);
        }
        return permutations;
    }

    public static void configureJob(Job job, String documentPath, String shinglingPath, String pathOut,
                                    int shinglingSize, int numBands, int signatureSize, int numHashBuckets,
                                    double jaccardThreshold) throws IOException {
        job.setJarByClass(MinHashLSH.class);
        assert (signatureSize % numBands) == 0 : "signatureSize have to be divisible by numBands";
        String[] permutationsBase64 = generatePermutationsArrays(shinglingPath, signatureSize);
        // Set the reducer class it must use
        job.setReducerClass(MinHashLSHReducer.class);


        // Set the mapper class it must use
        job.setMapperClass(MinHashLSHMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        // The output will be Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().setInt("shinglingSize", shinglingSize);
        job.getConfiguration().setInt("numBands", numBands);
        job.getConfiguration().setInt("signatureSize", signatureSize);
        job.getConfiguration().setInt("numHashBuckets", numHashBuckets);
        job.getConfiguration().setStrings("permutationsBase64", permutationsBase64);
        job.getConfiguration().setDouble("jaccardThreshold", jaccardThreshold);

        // This is for having a csv like output, instead space-separated key-values
        job.getConfiguration().set("mapred.textoutputformat.separator", ",");

        // add files to cache
        File file = new File(shinglingPath + "part-r-00000");
        job.addCacheFile(file.toURI());

        // Cleanup output path
        Path outputPath = new Path(pathOut);
        FileSystem fs = FileSystem.get(outputPath.toUri(), job.getConfiguration());
        fs.delete(outputPath, true);

        // The files the job will read from/write to
        FileInputFormat.addInputPath(job, new Path(documentPath));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));


    }

}
