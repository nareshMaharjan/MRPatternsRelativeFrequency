package com.hd.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



/**
 * Created with IntelliJ IDEA.
 * User: naresh
 * Date: 5/11/15
 * Time: 11:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class TextPair implements WritableComparable{
    protected Text first;
    protected Text second;
    protected String delim = ",";

    public TextPair() {
        first = new Text();
        second =  new Text();
    }

    public TextPair(String f, String s) {
        first = new Text();
        second =  new Text();
        set(f,s);
    }

    public void set(String first, String second) {
        this.first.set(first);
        this.second.set(second);
    }

    public Text getFirst() {
        return first;
    }
    public void setFirst(String first) {
        this.first .set(first);
    }
    public Text getSecond() {
        return second;
    }
    public void setSecond(String second) {
        this.second.set(second);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);

    }
    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    public String getKey(){
       return first.toString();

    }

    public String getValue(){
       return second.toString();
    }

    @Override
    public int compareTo(Object tePair) {
            TextPair tPair = (TextPair) tePair;
            int cmp = first.compareTo(tPair.getFirst());
            if (0 == cmp) {
                cmp = second.compareTo(tPair.getSecond());
            }
            return cmp;
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 198 + second.hashCode();
    }


    public boolean equals(Object obj) {
        boolean isEqual =  false;
        if (obj instanceof TextPair) {
            TextPair iPair = (TextPair)obj;
            isEqual = first.equals(iPair.first) && second.equals(iPair.second);
        }

        return isEqual;
    }


    public String toString() {
        return "" + first + delim + second;
    }
}
