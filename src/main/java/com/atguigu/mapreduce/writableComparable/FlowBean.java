package com.atguigu.mapreduce.writableComparable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-11-19 18:05
 **/



public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;
    private long dawnFlow;
    private long sumFlow;


    // 空参构造
    public FlowBean() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(dawnFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow =  in.readLong();
        this.dawnFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDawnFlow() {
        return dawnFlow;
    }

    public void setDawnFlow(long dawnFlow) {
        this.dawnFlow = dawnFlow;
    }

     public void setSumFlow() {
        this.sumFlow = this.dawnFlow + this.upFlow;
     }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + dawnFlow + "\t" + sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {

        return Long.compare(o.sumFlow, this.sumFlow);
    }
}
