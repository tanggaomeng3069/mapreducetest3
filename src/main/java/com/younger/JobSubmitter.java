package com.younger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobSubmitter {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration entries = new Configuration();

        Job job = Job.getInstance(entries);
        job.setJarByClass(JobSubmitter.class);

        job.setMapperClass(PageTopnMapper.class);
        job.setReducerClass(PageTopnReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("F:\\codedata\\mapreducetest3\\request.dat"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\codedata\\mapreducetest3\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:-1);
    }
}
