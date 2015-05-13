package com.hd.utils;

import org.apache.hadoop.io.Text;


/**
 * Created with IntelliJ IDEA.
 * User: naresh
 * Date: 5/12/15
 * Time: 11:02 AM
 * To change this template use File | Settings | File Templates.
 */
public class NumberPair extends TextPair{

    public NumberPair() {
        first = new Text();
        second =  new Text();
    }

    public NumberPair(String f, String s) {
        first = new Text();
        second =  new Text();
        set(f,s);
    }

    @Override
    public int compareTo(Object tePair) {
        NumberPair tPair = (NumberPair) tePair;
        Integer tePairInt = getKey();
        int cmp = tePairInt.compareTo(Integer.parseInt(tPair.getFirst().toString()));
        if (0 == cmp) {
            if(second.toString().equals("*") || tPair.getSecond().toString().equals("*"))
                cmp = second.compareTo(tPair.getSecond());
            else
                cmp=getValue().compareTo(Integer.parseInt(tPair.getSecond().toString()));
        }
        return cmp;
    }


    private Integer getKey(){
        return Integer.parseInt(first.toString());
    }

    private Integer getValue(){
        return Integer.parseInt(second.toString());
    }

}
