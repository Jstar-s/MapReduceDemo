package com.atguigu.mapreduce.partionerandwriteableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-11-28 13:19
 **/

public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }

    }
}
