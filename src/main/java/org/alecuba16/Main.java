package org.alecuba16;

// Main non-hadoop
// Read text document
// Generate shingling (ngrams) dict
// Generate randomized arrays for signatures

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Job 1
// Mapper:
// 1. Read each line document
// 2. Generate shingling (ngrams) dict
// Reducer:
// 1. Collect shingling

// Job 2
// Main distribute the shingling dict to all mappers
// Main distribute the randomized signature array
// Mapper:
// 1. Convert shingling dict to hashmap
// 2. Read a line of the document
// 3. Generate the one hot encoding from a base shingling array with 0, set 1 to the existing elements
// 4. Calculate the signature of the line
// 5. Generate the Hash for the N bands (the key)
// 6. Write that key and the sentence row id as the value.
// Reducer / Combiner:
// 1. Collect distinct sentence row id for each key
// 2. Generate csv with key column and key pairs
//
// Job 2
// Mapper:
// 1. Read a line of key pairs csv
// 2. Calculate the jacquard distance of each key pair, filter out the ones with distance < 0.5
// 3. Write to hash key and the byte offset of the matching sentence
// Reducer:
// 1. Collect all the key pairs and their distance
// 2. Generate a csv with key column and key pairs and distance as the value

// Job
public class Main extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        boolean success;
        String documentPath = "src/main/resources/documents.txt";
        String shinglingPath = "src/main/resources/results/shingling/";
        String minHashLshPath = "src/main/resources/results/minhashlsh/";
        String pairsPath = "src/main/resources/results/pairs/";
        int shinglingLength = 3;
        int numBands = 2;
        int signatureSize = numBands*2;
        int numHashBuckets = 5;
        double jaccardThreshold = 0.8;
        // Generate shingling
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Shingling");
        Shingling.configureJob(job, documentPath, shinglingPath,shinglingLength);
        success = job.waitForCompletion(true);
        if (!success) return 1;

        // Get hashes with equal lines
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "MinHashLSH");
        MinHashLSH.configureJob(job2, documentPath,shinglingPath, minHashLshPath,shinglingLength,numBands,signatureSize,
                numHashBuckets, jaccardThreshold);
        success = job2.waitForCompletion(true);
        if (!success) return 1;

        // Get hashes with equal lines
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "pairs");
        CollectCandidates.configureJob(job3, documentPath,minHashLshPath, pairsPath);
        success = job3.waitForCompletion(true);
        if (!success) return 1;

        return 0;
    }


    public static void main(String[] args) throws Exception {
        Main driver = new Main();
        int exitCode = ToolRunner.run(driver,args);
        System.exit(exitCode);
        System.out.println("Hello world!");
    }
}