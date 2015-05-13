package com.hd.crystalBall;

import com.hd.utils.NumberPair;
import com.hd.utils.StripeMapWritableCustom;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created with IntelliJ IDEA.
 * User: naresh
 * Date: 5/11/15
 * Time: 11:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class ComputeRelativeFrequency extends Configured implements Tool {
      // Todo custom output format
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.out.println("Not enough parameters");
            return -1;
        }

        String inputPath = args[1];
        String outputPath = args[2];

        int reduceTasks = Integer.parseInt(args[4]);

        Job job = new Job(getConf(), "ComputeRelativeFrequency");

        // Delete the output directory if it exists already
        Path outputDir = new Path(outputPath);
        FileSystem.get(getConf()).delete(outputDir, true);

        job.setJarByClass(ComputeRelativeFrequency.class);
        job.setNumReduceTasks(reduceTasks);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        prepareJob(job,args);


        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void prepareJob(Job job,String[] args){
        if(args[3].equals("pair")){
            job.setMapOutputKeyClass(NumberPair.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(NumberPair.class);
            job.setOutputValueClass(LongWritable.class);
            job.setMapperClass(RelFreqPairMapper.class);
//        job.setCombinerClass(MyReducer.class);
            job.setReducerClass(RelFreqPairReducer.class);
            job.setPartitionerClass(PairPartitioner.class);
        }  else if (args[3].equals("stripe")){
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(StripeMapWritableCustom.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(StripeMapWritableCustom.class);
            job.setMapperClass(RelFreqStripeMapper.class);
//        job.setCombinerClass(MyReducer.class);
            job.setReducerClass(RelFreqStripeReducer.class);
        } else if(args[3].equals("hybrid")) {
            job.setMapOutputKeyClass(NumberPair.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(StripeMapWritableCustom.class);
            job.setMapperClass(RelFreqHybridMapper.class);
//        job.setCombinerClass(MyReducer.class);
            job.setReducerClass(RelFreqHybridReducer.class);
            job.setPartitionerClass(PairPartitioner.class);
        }   else
            System.out.println("No algorithm found !!!!!!!!!!!!!!!!!!!");
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ComputeRelativeFrequency(), args);
        System.exit(res);
    }
}
