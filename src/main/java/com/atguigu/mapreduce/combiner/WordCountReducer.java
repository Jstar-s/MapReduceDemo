package com.atguigu.mapreduce.combiner;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-11-19 14:44
 **/

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *KEYIN, reduce阶段输入的key类型
 * VALUEIN,  reduce阶段输入的value类型
 * KEYOUT,   reduce阶段输出的key类型
 * VALUEOUT  reduce阶段输出的value类型
 */



public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {

            sum += value.get();
        }

        outV.set(sum);


        context.write(key, outV);

    }
}



















