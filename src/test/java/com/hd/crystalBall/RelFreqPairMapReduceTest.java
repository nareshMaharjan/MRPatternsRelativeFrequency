package com.hd.crystalBall;

import com.hd.utils.NumberPair;
import com.hd.utils.NumberPair;
import com.hd.wordCount.WordCount;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: naresh
 * Date: 5/12/15
 * Time: 10:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class RelFreqPairMapReduceTest {
    MapReduceDriver<LongWritable, Text, NumberPair, IntWritable, NumberPair, LongWritable> mapReduceDriver;
    MapDriver<LongWritable, Text, NumberPair, IntWritable> mapDriver;
    ReduceDriver<NumberPair, IntWritable, NumberPair, LongWritable> reduceDriver;

    @Before
    public void setUp() {
        RelFreqPairMapper mapper = new RelFreqPairMapper();
        RelFreqPairReducer reducer = new RelFreqPairReducer();
        mapDriver = new MapDriver<LongWritable, Text, NumberPair, IntWritable>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<NumberPair, IntWritable, NumberPair, LongWritable>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, NumberPair, IntWritable, NumberPair, LongWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new LongWritable(1), new Text("34 56 34 27"));
        mapDriver.withOutput(new NumberPair("34", "*"), new IntWritable(1));
        mapDriver.withOutput(new NumberPair("34", "56"), new IntWritable(1));
        mapDriver.withOutput(new NumberPair("56", "*"), new IntWritable(1));
        mapDriver.withOutput(new NumberPair("56", "34"), new IntWritable(1));
        mapDriver.withOutput(new NumberPair("56", "*"), new IntWritable(1));
        mapDriver.withOutput(new NumberPair("56", "27"), new IntWritable(1));
        mapDriver.withOutput(new NumberPair("34", "*"), new IntWritable(1));
        mapDriver.withOutput(new NumberPair("34", "27"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new NumberPair("34", "*"), values);
        reduceDriver.withInput(new NumberPair("34", "27"), Arrays.asList(new IntWritable(1)));
        reduceDriver.withInput(new NumberPair("34","56"), Arrays.asList(new IntWritable(1)));
        reduceDriver.withInput(new NumberPair("56","*"), Arrays.asList(new IntWritable(1), new IntWritable()));
        reduceDriver.withInput(new NumberPair("56","27"), Arrays.asList(new IntWritable(1)));
        reduceDriver.withInput(new NumberPair("56","34"), Arrays.asList(new IntWritable(1)));


        reduceDriver.withOutput(new NumberPair("34","27"), new LongWritable(1/2));
        reduceDriver.withOutput(new NumberPair("34","56"), new LongWritable(1/2));
        reduceDriver.withOutput(new NumberPair("56","27"), new LongWritable(1/2));
        reduceDriver.withOutput(new NumberPair("56","34"), new LongWritable(1/2));
//        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() {

        mapReduceDriver.withInput(new LongWritable(1), new Text("34 1000 34 27"));
        mapReduceDriver.withOutput(new NumberPair("34","27"), new LongWritable(1/2));
        mapReduceDriver.withOutput(new NumberPair("34","1000"), new LongWritable(1/2));
        mapReduceDriver.withOutput(new NumberPair("1000","27"), new LongWritable(1/2));
        mapReduceDriver.withOutput(new NumberPair("1000","34"), new LongWritable(1/2));;
        mapReduceDriver.runTest();
    }
}
