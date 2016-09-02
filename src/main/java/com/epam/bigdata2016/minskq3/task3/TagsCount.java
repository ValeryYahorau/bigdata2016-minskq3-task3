package com.epam.bigdata2016.minskq3.task3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TagsCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text tag = new Text();
        private Set<String> stopWords = new HashSet();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                // get file with stop words from distributed cache
                Path[] localPaths = context.getLocalCacheFiles();
                if (localPaths != null && localPaths.length > 0) {
                    for (Path stopWordFile : localPaths) {
                        readFile(stopWordFile);
                    }
                }
            } catch (IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            //check if line contains digits because first line containts only words with params names
            if (containsDigit(line)) {
                String[] params = line.split("\\s+");

                //tags on second position in line
                String[] tags = params[1].toUpperCase().split(",");
                for (String currentTag : tags) {
                    if (!stopWords.contains(currentTag)) {
                        tag.set(currentTag);
                        context.write(tag, one);
                    }
                }
            }
        }

        private void readFile(Path filePath) {
            try {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
                String stopWord = null;
                while ((stopWord = bufferedReader.readLine()) != null) {
                    stopWords.add(stopWord.toUpperCase());
                }
            } catch (IOException ex) {
                System.err.println("Exception while reading stop words file: " + ex.getMessage());
            }
        }

        public final boolean containsDigit(String s) {
            boolean containsDigit = false;

            if (s != null && !s.isEmpty()) {
                for (char c : s.toCharArray()) {
                    if (containsDigit = Character.isDigit(c)) {
                        break;
                    }
                }
            }
            return containsDigit;
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //otherArgs = new String[]{"/Users/valeryyegorov/Downloads/testin1.txt", "/Users/valeryyegorov/Downloads/testout1.txt"};

        if (otherArgs.length < 2) {
            System.err.println("Usage: tagscount <in> <out> [<in>...]");
            System.exit(2);
        }
        Job job = new Job(conf, "Tags count");
        job.setJarByClass(TagsCount.class);
        job.setMapperClass(TokenizerMapper.class);

        // for current task Combiner is the same like Reducer
        // take a look at http://www.tutorialspoint.com/map_reduce/map_reduce_combiners.htm
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        if (otherArgs.length > 2) {
            // put input file with stop words to distributed cache
            job.addCacheFile(new Path(otherArgs[2]).toUri());
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}