package com.hd.crystalBall;

import com.hd.utils.StripeMapWritableCustom;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: naresh
 * Date: 5/11/15
 * Time: 11:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class RelFreqStripeReducer extends Reducer<Text, StripeMapWritableCustom, Text, StripeMapWritableCustom> {

    @Override
    public void reduce(Text keyin, Iterable<StripeMapWritableCustom> valuein, Context context) throws IOException, InterruptedException {
        Iterator<StripeMapWritableCustom> iter = valuein.iterator();
        System.out.println("key in:::: "+keyin);
        StripeMapWritableCustom mw=new StripeMapWritableCustom();
        double total = 0;
        while (iter.hasNext()) {
            StripeMapWritableCustom smw=iter.next();
            for(Map.Entry<String,Double> m: smw.entrySet()){
                total=total + m.getValue();
                mw.increment(m.getKey(),m.getValue());
            }
        }
        for(String k: mw.keySet()){
               mw.put(k,mw.get(k)/total);
        }

        context.write(keyin,mw);
    }
}
