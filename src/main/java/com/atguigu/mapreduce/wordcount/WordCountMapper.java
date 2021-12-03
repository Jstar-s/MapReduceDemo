package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.swing.tree.RowMapper;
import java.io.IOException;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-11-19 14:44
 **/

/**
 *KEYIN, map阶段输入的key类型
 * VALUEIN,  map阶段输入的value类型
 * KEYOUT,   map阶段输出的key类型
 * VALUEOUT  map阶段输出的value类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // get a line and covert to string
        String line = value.toString();

        // split by " "
        String[] words = line.split(" ");


        for (String word: words) {

            outK.set(word);

            context.write(outK, outV);
        }




    }
}
