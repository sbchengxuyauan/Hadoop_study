package com.hadoop.zym.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class Myjob extends Configured {   //Tool支持处理命令行模式
        public static  void main(String[] args) throws Exception {
          Myjob myjob=new Myjob();
          Configuration conf=new Configuration();  //配置信息
          myjob.before(conf);
          String in="/test/wordcount.txt";
          String out="/test/outfile";
          Job job=Job.getInstance(conf,"MAXVLAUE");//注意job的执行顺序,job就是管理task任务
          System.setProperty("Hadoop_user_name","root");
          job.setJarByClass(Myjob.class);  //job的主类
          job.setMapperClass(Mymapper.class);
          job.setCombinerClass(MyReducer.class);  //Combiner在本地端对mapper的输出结果进行一次集合归并,其输出是reducer的输人,其是MR的三大组件之一
          job.setReducerClass(MyReducer.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(IntWritable.class);
          FileInputFormat.addInputPath(job,new Path(in));
          FileOutputFormat.setOutputPath(job,new Path(out));
          myjob.deletefile(out,conf);
          // job.submit(); 工作提交
          try {
              Boolean boo = job.waitForCompletion(true); //等待执行
              System.out.println(boo);
          }catch (Exception e){
              e.printStackTrace();
              System.out.println(e.getMessage());
          }
      }

      public void before(Configuration conf){
              conf.set("fs.default.name","hdfs://192.168.147.130:9000");//设置HDFS节点
      }
      public void deletefile(String file,Configuration conf){
         Path path=new Path(file);
          try {
              FileSystem fileSystem=FileSystem.get(conf);
              if(fileSystem.exists(path)){
                  fileSystem.delete(path,true);
              }

          } catch (IOException e) {
              e.printStackTrace();
              System.out.println(e.getMessage());
          }

      }
}
