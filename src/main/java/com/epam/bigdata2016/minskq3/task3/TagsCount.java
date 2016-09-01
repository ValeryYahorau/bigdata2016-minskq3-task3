package com.epam.bigdata2016.minskq3.task3;

import java.io.IOException;

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

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String inputText = value.toString();
            String[] lines = inputText.split("\\n");

            // skip first line because it contains names of parameters
            for (int i = 1; i < lines.length; i++) {

                String currentLine = lines[i];
                String[] params = currentLine.split("\\s+");
                String[] tags = params[1].toUpperCase().split(",");
                for (String currentTag : tags) {
                    tag.set(currentTag);
                    context.write(tag, one);
                }
            }
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
        if (otherArgs.length < 2) {
            System.err.println("Usage: tagscount <in> [<in>...] <out>");
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
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));

            // put input file to distributed cache
            job.addCacheFile(new Path(otherArgs[i]).toUri());
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}