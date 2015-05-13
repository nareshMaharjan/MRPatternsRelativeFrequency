package com.hd.crystalBall;

import com.hd.utils.NumberPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: naresh
 * Date: 5/13/15
 * Time: 12:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class RelFreqHybridMapper  extends Mapper<LongWritable, Text, NumberPair, IntWritable> {
    @Override
    public void setup(Context context){

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
                System.out.println("tp:: "+tp);
                context.write(tp,new IntWritable(1));
            }
        }

    }

    @Override
    public void cleanup(Context context){
        System.out.println("The mapper clean up");
    }
}
