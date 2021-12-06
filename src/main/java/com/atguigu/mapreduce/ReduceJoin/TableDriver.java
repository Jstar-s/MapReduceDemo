package com.atguigu.mapreduce.ReduceJoin;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-12-06 16:16
 **/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(TableDriver.class);
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/Volumes/DATA/code/javacode/MapReduceDemo/src/main/resources/inputtable"));
        FileOutputFormat.setOutputPath(job, new Path("/Volumes/DATA/code/javacode/MapReduceDemo/src/main/resources/tableBeanOut"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}