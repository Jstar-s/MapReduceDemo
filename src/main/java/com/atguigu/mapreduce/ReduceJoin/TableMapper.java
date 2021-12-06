package com.atguigu.mapreduce.ReduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-12-06 15:49
 **/

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {


    private String fileName;

    Text outK = new Text();

    TableBean outV = new TableBean();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();

    }


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {

        String line = value.toString();


        if (fileName.contains("order")) {

            String[] split = line.split("\t");

            outK.set(split[1]);
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");

        } else {

            String[] split = line.split("\t");

            outK.set(split[0]);

            outV.setId("");
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");

        }

        context.write(outK, outV);

    }
}
