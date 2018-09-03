package com.hadoop.hbase.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HbaseMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    public void map(LongWritable longWritable, Text text,Context context)  throws IOException, InterruptedException{
        String line =text.toString();
        String year =line.substring(0,4);
        System.out.println(line);
        String data=line.substring(7,9);
        System.out.println(data);
        context.write(new Text(year),new IntWritable(Integer.valueOf(data)));
    }
}
