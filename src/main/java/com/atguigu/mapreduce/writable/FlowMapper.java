package com.atguigu.mapreduce.writable;

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

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private  FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        System.out.println("keyIN: " + key);

        // get a line;
        String line = value.toString();

        // split

        String[] split = line.split("\t");


        String phone = split[1];

        String up = split[split.length -3];

        String down = split[split.length - 2];


        outK.set(phone);

        outV.setUpFlow(Long.parseLong(up));
        outV.setDawnFlow(Long.parseLong(down));
        outV.setSumFlow();


        context.write(outK, outV);




    }
}
