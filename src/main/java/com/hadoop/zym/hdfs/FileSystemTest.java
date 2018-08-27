package com.hadoop.zym.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;
import java.net.URL;
import java.util.Scanner;

public class FileSystemTest {

   public static  void main(String[] args){
       String url = null;
       Scanner scanner=new Scanner(System.in);
       System.out.println("请输入HDFS文件地址:");
       if (scanner.hasNext()){
          url=scanner.nextLine();
       }

       Configuration conf=new Configuration();//读取配置对象
       }
   }




