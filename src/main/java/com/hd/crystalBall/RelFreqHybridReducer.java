package com.hd.crystalBall;

import com.hd.utils.NumberPair;
import com.hd.utils.StripeMapWritableCustom;
import org.apache.hadoop.io.IntWritable;
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
public class RelFreqHybridReducer extends Reducer<NumberPair, IntWritable, IntWritable, StripeMapWritableCustom> {
    StripeMapWritableCustom smwc;
    Integer lastPairKey;
    Double total;

    @Override
    public void setup(Context context){
        total=0d;
        smwc = new StripeMapWritableCustom();
        lastPairKey = null;
    }


    @Override
    public void reduce(NumberPair keyin, Iterable<IntWritable> valuein, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iter = valuein.iterator();
        System.out.println("key in:::: "+keyin);

        if(lastPairKey==null)
            lastPairKey=keyin.getNumPairKey();

        if(lastPairKey!=keyin.getNumPairKey()){
            for(String s: smwc.keySet()){
                smwc.put(s,smwc.get(s)/total);
            }

            context.write(new IntWritable(lastPairKey),smwc);

            total=0d;
            smwc=new StripeMapWritableCustom();
            lastPairKey=keyin.getNumPairKey();
        }

        Double sum=0d;

        while (iter.hasNext()) {
            sum=sum + iter.next().get();
        }
        smwc.put(keyin.getValue(), sum);
        total=total+sum;
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for(String s: smwc.keySet()){
            smwc.put(s,smwc.get(s)/total);
        }

        context.write(new IntWritable(lastPairKey),smwc);
    }
}
