package com.hd.crystalBall;

import com.hd.utils.NumberPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created with IntelliJ IDEA.
 * User: naresh
 * Date: 5/13/15
 * Time: 10:42 AM
 * To change this template use File | Settings | File Templates.
 */
public class PairPartitioner extends Partitioner<NumberPair,IntWritable> {

    @Override
    public int getPartition(NumberPair numberPair, IntWritable intWritable, int numPartitions) {
        return numberPair.getNumPairKey() % numPartitions;
    }
}
