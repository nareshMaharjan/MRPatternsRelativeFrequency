package com.hd.crystalBall;

import com.hd.utils.NumberPair;
import com.hd.utils.StripeMapWritableCustom;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: naresh
 * Date: 5/11/15
 * Time: 11:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class RelFreqPairMapper extends Mapper<LongWritable, Text, NumberPair, IntWritable> {
    HashMap<NumberPair,Integer> H;

    @Override
    public void setup(Context context){
        H =new HashMap<NumberPair,Integer>();
    }

    @Override
    public void map(LongWritable keyin, Text line, Context context) throws IOException, InterruptedException {
        String text = line.toString();
        String[] terms = text.split("\\s+");
        System.out.println("The input line :::: "+line);
        for (int i=0;i<terms.length;i++){
            for(int j=i+1;j<terms.length;j++){
                if(terms[i].equals(terms[j])) break;
//                NumberPair tp=new NumberPair(Long.parseLong(terms[i]),Long.parseLong(terms[j]));
//                NumberPair tpt=new NumberPair(Long.parseLong(terms[i]));
                NumberPair tp = new NumberPair(terms[i],terms[j]);
                NumberPair tpt = new NumberPair(terms[i],"*");
                System.out.println("tp:: "+tp+"   tpt::  "+tpt);
                addElement(tp);
                addElement(tpt);
//                context.write(tpt,new IntWritable(1));
//                context.write(tp,new IntWritable(1));
            }
        }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("The mapper clean up");
        for(Map.Entry<NumberPair,Integer> m: H.entrySet()){
            context.write(m.getKey(),new IntWritable(m.getValue()));
        }
    }

    public void addElement(NumberPair n){
        if (H.containsKey(n)) {
            H.put(n, H.get(n) + 1);
        } else {
            H.put(n, 1);
        }
    }
}
