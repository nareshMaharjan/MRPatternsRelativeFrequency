package com.hd.crystalBall;

import com.hd.utils.NumberPair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: naresh
 * Date: 5/11/15
 * Time: 11:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class RelFreqPairReducer extends Reducer<NumberPair, IntWritable,NumberPair,DoubleWritable> {
    Double total=0d;
    DoubleWritable relFreq;

    @Override
    public void setup(Context context){
//        total=0l;
        relFreq=new DoubleWritable(0);

    }

    @Override
    public void reduce(NumberPair keyin, Iterable<IntWritable> valuein, Context context) throws IOException, InterruptedException {
        Double sum = 0d;
        Iterator<IntWritable> iter = valuein.iterator();
        System.out.println("key in:::: "+keyin);

        while (iter.hasNext()) {
            sum += iter.next().get();
        }
        if(keyin.getSecond().toString().equals("*")){
            total=sum;
        } else {
            relFreq.set(sum/total);
            context.write(keyin, relFreq);
        }
    }

    @Override
    public void cleanup(Context context){
        System.out.println("The reducer clean up");
    }

}

