package com.hd.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Created with IntelliJ IDEA.
 * User: naresh
 * Date: 5/12/15
 * Time: 11:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class StripeMapWritable extends MapWritable {
    public StripeMapWritable() {
        super();
    }


    public static StripeMapWritable create(DataInput in) throws IOException {
        StripeMapWritable m = new StripeMapWritable();
        m.readFields(in);

        return m;
    }

    public void plus(StripeMapWritable m) {
        for (Map.Entry<Writable, Writable> e : m.entrySet()) {
            Text key = (Text)e.getKey();
            if (this.containsKey(key)) {
                this.put(key, new IntWritable(((IntWritable)this.get(key)).get() + ((IntWritable)e.getValue()).get()));
            } else {
                this.put(key, e.getValue());
            }
        }
    }

//to increment the value
    public void increment(Text key) {
        increment(key, 1);
    }


    public void increment(Text key, int n) {
        if (this.containsKey(key)) {
            this.put(key, new IntWritable(((IntWritable)this.get(key)).get() + n));
        } else {
            this.put(key, new IntWritable(n));
        }
    }
}
