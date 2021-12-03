package com.atguigu.mapreduce.partionerandwriteableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-12-02 21:09
 **/

public class ProvincePartitioner2 extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {

        String phone = text.toString();

        if (phone.startsWith("136")) {
            return 0;
        } else if (phone.startsWith("137")){
            return 1;
        } else if (phone.startsWith("138")) {
            return 2;
        } else if (phone.startsWith("139")) {
            return 3;
        } else {
            return 4;
        }
    }
}
