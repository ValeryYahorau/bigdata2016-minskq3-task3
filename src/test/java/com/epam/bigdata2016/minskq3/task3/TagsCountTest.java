package com.epam.bigdata2016.minskq3.task3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TagsCountTest {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    private final String input1 = "282163091263 audi,bmw,mercedes,mazda ON CPC BROAD http://dummy.com";
    private final String input2 = "282163091263 mercedes,mazda ON CPC BROAD http://dummy.com";
    private final String input3 = "282163091263 mazda ON CPC BROAD http://dummy.com";

    private final String s1 = "AUDI";
    private final String s2 = "BMW";
    private final String s3 = "MERCEDES";
    private final String s4 = "MAZDA";

    @Before
    public void setUp() {
        TagsCount.TokenizerMapper mapper = new TagsCount.TokenizerMapper();
        TagsCount.IntSumReducer reducer = new TagsCount.IntSumReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(input1));
        mapDriver.withInput(new LongWritable(), new Text(input2));
        mapDriver.withInput(new LongWritable(), new Text(input3));
        mapDriver.withOutput(new Text(s1), new IntWritable(1));
        mapDriver.withOutput(new Text(s2), new IntWritable(1));
        mapDriver.withOutput(new Text(s3), new IntWritable(1));
        mapDriver.withOutput(new Text(s4), new IntWritable(1));
        mapDriver.withOutput(new Text(s3), new IntWritable(1));
        mapDriver.withOutput(new Text(s4), new IntWritable(1));
        mapDriver.withOutput(new Text(s4), new IntWritable(1));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values1 = new ArrayList<IntWritable>();
        values1.add(new IntWritable(1));
        reduceDriver.withInput(new Text(s1), values1);

        List<IntWritable> values2 = new ArrayList<IntWritable>();
        values2.add(new IntWritable(1));
        reduceDriver.withInput(new Text(s2), values2);

        List<IntWritable> values3 = new ArrayList<IntWritable>();
        values3.add(new IntWritable(1));
        values3.add(new IntWritable(1));
        reduceDriver.withInput(new Text(s3), values3);

        List<IntWritable> values4 = new ArrayList<IntWritable>();
        values4.add(new IntWritable(1));
        values4.add(new IntWritable(1));
        values4.add(new IntWritable(1));
        reduceDriver.withInput(new Text(s4), values4);

        reduceDriver.withOutput(new Text(s1), new IntWritable(1));
        reduceDriver.withOutput(new Text(s2), new IntWritable(1));
        reduceDriver.withOutput(new Text(s3), new IntWritable(2));
        reduceDriver.withOutput(new Text(s4), new IntWritable(3));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(input1));
        mapReduceDriver.withInput(new LongWritable(), new Text(input2));
        mapReduceDriver.withInput(new LongWritable(), new Text(input3));

        mapReduceDriver.withOutput(new Text(s1), new IntWritable(1));
        mapReduceDriver.withOutput(new Text(s2), new IntWritable(1));
        mapReduceDriver.withOutput(new Text(s4), new IntWritable(3));
        mapReduceDriver.withOutput(new Text(s3), new IntWritable(2));

        mapReduceDriver.runTest();
    }
}