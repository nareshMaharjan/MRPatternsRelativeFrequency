package com.hd.utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: naresh
 * Date: 5/12/15
 * Time: 11:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class StripeMapWritableCustom extends HashMap<String,Double> implements Writable {

    //Serialize
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(size());
        for (String k : keySet()) {
            out.writeUTF(k);
            out.writeDouble(get(k));
        }
    }

    //Deserialize
    @Override
    public void readFields(DataInput in) throws IOException {
        clear();
        int size = in.readInt();
        for (int i=0;i<size;i++) {
            String k=in.readUTF();
            Double v = in.readDouble();
            put(k,v);
        }
    }



    public void increment(String key) {
        increment(key, 1);
    }


    public void increment(String key, double n) {
        if (this.containsKey(key)) {
            this.put(key, this.get(key) + n);
        } else {
            this.put(key, n);
        }
    }


}

