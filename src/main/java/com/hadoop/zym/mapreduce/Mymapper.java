package com.hadoop.zym.mapreduce;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class Mymapper extends Mapper<LongWritable,Text,Text,IntWritable> implements org.apache.hadoop.mapred.Mapper {



    @Override
    public void map(LongWritable longWritable, Text text,Context context)  throws IOException, InterruptedException{
      String line =text.toString();
      String year =line.substring(0,4);
      System.out.println(line);
      String data=line.substring(7,9);
      System.out.println(data);
      context.write(new Text(year),new IntWritable(Integer.valueOf(data)));
    }

    /**
     * 该方法会去执行重写的map方法,进而可以控制相应的执行流程
     * @param context
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException {
      super.run(context);
      System.out.println(context.getCurrentKey()+"："+context.getCurrentValue());
    }


    @Override
    public void map(Object o, Object o2, OutputCollector outputCollector, Reporter reporter) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }
}



