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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IndexStepOne {

    // 产生<hello-文件名, 1>
    public static class IndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 从输入切片信息中获取当前正在处理的一行数据所属的文件
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String name = inputSplit.getPath().getName();

            String[] split = value.toString().split(" ");
            for (String word:split){
                context.write(new Text(word + '-' + name), new IntWritable(1));
            }
        }
    }

    public static class IndexStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value:values){
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 默认只加载core-default.xml core-site.xml
        Configuration entries = new Configuration();

        Job job = Job.getInstance(entries);

        job.setJarByClass(IndexStepOne.class);

        job.setMapperClass(IndexStepOneMapper.class);
        job.setReducerClass(IndexStepOneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("F:\\codedata\\mapreducetest3\\indexData"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\codedata\\mapreducetest3\\indexOutput1"));

        job.setNumReduceTasks(3);

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:-1);

    }
}
