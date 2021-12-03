package com.atguigu.mapreduce.writableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-11-28 13:05
 **/

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean outK = new FlowBean();
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // get a line;
        String line = value.toString();

        // split

        String[] split = line.split("\t");


        outV.set(split[0]);

        outK.setUpFlow(Long.parseLong(split[1]));
        outK.setDawnFlow(Long.parseLong(split[2]));
        outK.setSumFlow();

        context.write(outK, outV);

    }
}
