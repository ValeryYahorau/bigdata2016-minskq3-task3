package com.epam.bigdata2016.minskq3.task3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class VisitsSpendsCountTest {

    MapDriver<Object, Text, Text, VisitSpendComparable> mapDriver;
    ReduceDriver<Text, VisitSpendComparable, Text, VisitSpendComparable> reduceDriver;
    MapReduceDriver<Object, Text, Text, VisitSpendComparable, Text, VisitSpendComparable> mapReduceDriver;

    private final String input1 = "8c66f1538798b7ab57e2da7be11c5696	20130606222224943	Z0KpO7S8PQpNDBa	Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)	116.1.44.*	...	300	250	0	0	100	e1af08818a6cd6bbba118bb54a651961	254	3476	282825712806	0";
    private final String input2 = "8c66f1538798b7ab57e2da7be11c5696	20130606222224943	Z0KpO7S8PQpNDBa	Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)	116.1.44.*	...	300	250	0	0	100	e1af08818a6cd6bbba118bb54a651961	254	3476	282825712806	0";
    private final String input3 = "8c66f1538798b7ab57e2da7be11c5696	20130606222224943	Z0KpO7S8PQpNDBa	Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)	116.1.45.*	...	300	250	0	0	100	e1af08818a6cd6bbba118bb54a651961	254	3476	282825712806	0";

    private final String ip1 = "116.1.44.*";
    private final String ip2 = "116.1.45.*";

    @Before
    public void setUp() {
        VisitsSpendsCount.VisitsSpendsMapper mapper = new VisitsSpendsCount.VisitsSpendsMapper();
        VisitsSpendsCount.VisitsSpendsReducer reducer = new VisitsSpendsCount.VisitsSpendsReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(input1));
        mapDriver.withInput(new LongWritable(), new Text(input2));
        mapDriver.withInput(new LongWritable(), new Text(input3));
        mapDriver.withOutput(new Text(ip1), new VisitSpendComparable(1, 254));
        mapDriver.withOutput(new Text(ip1), new VisitSpendComparable(1, 254));
        mapDriver.withOutput(new Text(ip2), new VisitSpendComparable(1, 254));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<VisitSpendComparable> values1 = new ArrayList<VisitSpendComparable>();
        values1.add(new VisitSpendComparable(1, 254));
        values1.add(new VisitSpendComparable(1, 254));
        reduceDriver.withInput(new Text(ip1), values1);

        List<VisitSpendComparable> values2 = new ArrayList<VisitSpendComparable>();
        values2.add(new VisitSpendComparable(1, 254));
        reduceDriver.withInput(new Text(ip2), values2);

        reduceDriver.withOutput(new Text(ip1), new VisitSpendComparable(2, 508));
        reduceDriver.withOutput(new Text(ip2), new VisitSpendComparable(1, 254));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(input1));
        mapReduceDriver.withInput(new LongWritable(), new Text(input2));
        mapReduceDriver.withInput(new LongWritable(), new Text(input3));

        mapReduceDriver.withOutput(new Text(ip1), new VisitSpendComparable(2, 508));
        mapReduceDriver.withOutput(new Text(ip2), new VisitSpendComparable(1, 254));

        mapReduceDriver.runTest();
    }
}