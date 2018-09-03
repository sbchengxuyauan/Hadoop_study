package com.hadoop.hbase.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HbaseReducer extends Reducer <Text,IntWritable,Text,IntWritable> {

    @Override
    public void reduce(Text text, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
