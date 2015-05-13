package com.hd.crystalBall;

import com.hd.utils.StripeMapWritableCustom;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: naresh
 * Date: 5/11/15
 * Time: 11:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class RelFreqStripeMapper extends Mapper<LongWritable,Text,Text,StripeMapWritableCustom>{
    @Override
    public void map(LongWritable key, Text line, Context context) throws IOException,
            InterruptedException {
        String text = line.toString();
        String[] terms = text.split("\\s+");
        System.out.println("The input line :::: "+line);
        for (int i=0;i<terms.length;i++){
            StripeMapWritableCustom mw=new StripeMapWritableCustom();
            for(int j=i+1;j<terms.length;j++){
                if(terms[i].equals(terms[j])) break;
                mw.increment(terms[j]);
            }
            context.write(new Text(terms[i]),mw);
        }
    }
}
