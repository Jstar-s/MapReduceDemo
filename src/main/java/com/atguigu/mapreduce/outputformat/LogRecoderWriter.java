package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @program: MapReduceDemo
 * @description:
 * @author: zjh
 * @create: 2021-12-03 13:46
 **/

@SuppressWarnings("unused")
public class LogRecoderWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream atguigu;
    private FSDataOutputStream other;

    public LogRecoderWriter(TaskAttemptContext job) {

        try {
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());
            atguigu = fileSystem.create(new Path("/Volumes/DATA/code/javacode/MapReduceDemo/src/main/resources/Format/atguigu.log"));
            other = fileSystem.create(new Path("/Volumes/DATA/code/javacode/MapReduceDemo/src/main/resources/Format/other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        // 具体写
        String log = key.toString();

        if (log.contains("atguigu")) {
            atguigu.writeBytes(log + "\n");
        } else {
            other.writeBytes(log + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
         IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}
