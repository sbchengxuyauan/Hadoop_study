package com.zym.pojo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Student   implements Writable {

    private  int age;
    private  String name;
    private  String jobname;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJobname() {
        return jobname;
    }

    public void setJobname(String jobname) {
        this.jobname = jobname;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        IntWritable hadoopage=new IntWritable(age);
        hadoopage.write(dataOutput);
        Text hadoopname=new Text(name);
        hadoopname.write(dataOutput);
        Text hadoopjop=new Text(jobname);
        hadoopjop.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
