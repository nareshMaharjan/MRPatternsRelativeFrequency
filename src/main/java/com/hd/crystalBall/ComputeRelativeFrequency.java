package com.hd.crystalBall;

import com.hd.utils.NumberPair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Not enough parameters");
            return -1;
        }

        String inputPath = args[0];
        String outputPath = args[1];

        int window = Integer.parseInt(args[2]);
        int reduceTasks = Integer.parseInt(args[3]);

        Job job = new Job(getConf(), "ComputeRelativeFrequency");

        // Delete the output directory if it exists already
        Path outputDir = new Path(outputPath);
        FileSystem.get(getConf()).delete(outputDir, true);

        job.setJarByClass(ComputeRelativeFrequency.class);
        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(NumberPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NumberPair.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setMapperClass(MyMapper.class);
//        job.setCombinerClass(MyReducer.class);
//        job.setReducerClass(MyReducer.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ComputeRelativeFrequency(), args);
        System.exit(res);
    }
}
