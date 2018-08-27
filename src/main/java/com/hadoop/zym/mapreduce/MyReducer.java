package com.hadoop.zym.mapreduce;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


/**
 * reduce程序
 */
public class MyReducer  extends Reducer<Text,IntWritable,Text,IntWritable> {  //泛型 输入类型，输出类型

    @Override
    public void reduce(Text text,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int MAX=0;
        while(values.iterator().hasNext()){
            int K=values.iterator().next().get();
            System.out.println(K);
            if(K>=MAX){
               MAX=K;
           }
        }
        context.write(text,new IntWritable(MAX));
    }


}
