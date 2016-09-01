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

public class VisitsSpendsCount {

    public static class VisitsSpendsMapper extends Mapper<Object, Text, Text, VisitSpendComparable> {

        private Text ipText = new Text();
        private VisitSpendComparable vsc = new VisitSpendComparable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            System.out.println("Step0 current line : " + line);
            String[] params = line.split("\\s+");

            String ip = params[4];
            ipText.set(ip);

            int bp = Integer.getInteger(params[params.length - 3]);
            vsc.setSpendsCount(bp);
            vsc.setVisitsCount(1);

            context.write(ipText, vsc);
        }
    }


    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, VisitSpendComparable> {
        private VisitSpendComparable result = new VisitSpendComparable();

        public void reduce(Text key, Iterable<VisitSpendComparable> values, Context context) throws IOException, InterruptedException {
            int visitCount = 0;
            int sumbidPrice = 0;
            for (VisitSpendComparable val : values) {
                visitCount += val.getVisitsCount();
                sumbidPrice += val.getSpendsCount();
            }
            result.setVisitsCount(visitCount);
            result.setSpendsCount(sumbidPrice);
            context.write(key, result);
        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: VisitsSpendsCount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Visits Spends count");
        job.setJarByClass(TagsCount.class);
        job.setMapperClass(VisitsSpendsMapper.class);

        // for current task Combiner is the same like Reducer
        // take a look at http://www.tutorialspoint.com/map_reduce/map_reduce_combiners.htm
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}