package com.atguigu.mapreduce.writableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-11-28 13:27
 **/

public class FlowDriver {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {


        Configuration config = new Configuration();

        Job job = Job.getInstance(config);


        job.setJarByClass(FlowDriver.class);


        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        FileInputFormat.setInputPaths(job, new Path("/Volumes/DATA/code/javacode/MapReduceDemo/src/main/resources/writeComparable"));
        FileOutputFormat.setOutputPath(job, new Path("/Volumes/DATA/code/javacode/MapReduceDemo/src/main/resources/Comparable"));


        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
