package com.atguigu.mapreduce.outputformat;

import com.atguigu.mapreduce.combiner.WordCountCombiner;
import com.atguigu.mapreduce.combiner.WordCountDriver;
import com.atguigu.mapreduce.combiner.WordCountMapper;
import com.atguigu.mapreduce.combiner.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-12-03 14:21
 **/

public class LogDriver {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1 get job

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        // 2 设置jar包路径
        job.setJarByClass(LogDriver.class);


        // 3 关联mapper和reducer
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        // 4 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5 设置最终输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置自定义的outputFormat
        job.setOutputFormatClass(LogOutputFormat.class);

        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("/Volumes/DATA/code/javacode/MapReduceDemo/src/main/resources/log.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Volumes/DATA/code/javacode/MapReduceDemo/src/main/resources/LogFormat"));


        // 7 提交job
        boolean result = job.waitForCompletion(true);


        System.exit(result? 0 : 1);


    }
}
