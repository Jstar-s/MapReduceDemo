package com.atguigu.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-11-28 13:19
 **/

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean outV = new FlowBean();


    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long totalUp = 0;
        long totalDown = 0;
        long totalSum = 0;

        for (FlowBean value : values) {
            totalUp += value.getUpFlow();
            totalDown += value.getDawnFlow();
            totalSum += value.getSumFlow();
        }

        outV.setUpFlow(totalUp);
        outV.setDawnFlow(totalDown);
        outV.setSumFlow(totalSum);

        context.write(key, outV);
    }
}
