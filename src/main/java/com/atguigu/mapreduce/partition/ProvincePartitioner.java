package com.atguigu.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-11-28 20:35
 **/

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {


    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {

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
