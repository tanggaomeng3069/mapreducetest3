package com.younger.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IndexStepTwo {

    public static class IndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("-");
            context.write(new Text(split[0]), new Text(split[1].replaceAll("\t", "-->")));
        }
    }

    public static class IndexStepTwoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // stringbuffer是线程安全的，stringbuilder是非线程安全的，在不涉及线程安全的情况下，stringbuilter更快
            StringBuilder stringBuilder = new StringBuilder();
            for (Text value:values){
                stringBuilder.append(value.toString()).append("\t");
            }
            context.write(key, new Text(stringBuilder.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);

        job.setJarByClass(IndexStepTwo.class);

        job.setMapperClass(IndexStepTwoMapper.class);
        job.setReducerClass(IndexStepTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("F:\\codedata\\mapreducetest3\\indexOutput1"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\codedata\\mapreducetest3\\indexOutput2"));

        job.setNumReduceTasks(1);

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:-1);
    }


}
