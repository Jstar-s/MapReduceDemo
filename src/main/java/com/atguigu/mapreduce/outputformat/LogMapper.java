package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-12-03 13:31
 **/

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {


        //  不进行任何处理，value值取空
        context.write(value, NullWritable.get());
    }
}
