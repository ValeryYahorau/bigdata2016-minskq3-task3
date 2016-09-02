package com.epam.bigdata2016.minskq3.task3;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class VisitsSpendsCount {

    public static class VisitsSpendsMapper extends Mapper<Object, Text, Text, VisitSpendComparable> {

        private Text ipText = new Text();
        private VisitSpendComparable vsc = new VisitSpendComparable();
        // regex for ip
        Pattern pIp = Pattern.compile("\\s\\d+\\.\\d+\\.\\d+\\.(\\d+|\\*)\\s");
        // regex for first 3 params and then User Agent
        Pattern pUserAgent = Pattern.compile("[a-zA-Z0-9]+\\s[0-9]+\\s[a-zA-Z0-9]+\\s(.*)");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] params = line.split("\\s+");
            Matcher m = pIp.matcher(line);
            if (m.find()) {
                String ip = m.group().trim();
                ipText.set(ip);
                Integer bp = Integer.parseInt(params[params.length - 4]);

                vsc.setSpendsCount(bp.intValue());
                vsc.setVisitsCount(1);
                context.write(ipText, vsc);

                String firstParams = line.split(ip)[0].trim();

                Matcher m2 = pUserAgent.matcher(firstParams);
                if (m2.find()) {
                    String userAgent = m2.group(1);
                    UserAgent ua = new UserAgent(userAgent);
                    // increment counter
                    context.getCounter(ua.getBrowser()).increment(1);

                }
            }
        }
    }

    public static class VisitsSpendsReducer extends Reducer<Text, VisitSpendComparable, Text, VisitSpendComparable> {
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
        //otherArgs = new String[]{"/Users/valeryyegorov/Downloads/testin2.txt", "/Users/valeryyegorov/Downloads/testout2.txt"};

        if (otherArgs.length < 2) {
            System.err.println("Usage: VisitsSpendsCount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Visits Spends count");
        job.setJarByClass(VisitsSpendsCount.class);
        job.setMapperClass(VisitsSpendsMapper.class);

        // for current task Combiner is the same like Reducer
        // take a look at http://www.tutorialspoint.com/map_reduce/map_reduce_combiners.htm
        job.setCombinerClass(VisitsSpendsReducer.class);
        job.setReducerClass(VisitsSpendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VisitSpendComparable.class);

        // compression with Snappy
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        boolean result = job.waitForCompletion(true);

        System.out.println("Browsers Counter :");
        for (Counter counter : job.getCounters().getGroup(Browser.class.getCanonicalName())) {
            System.out.println(" - " + counter.getDisplayName() + ": " + counter.getValue());
        }

        System.exit(result ? 0 : 1);
    }
}