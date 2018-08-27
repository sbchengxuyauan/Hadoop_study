package com.hadoop.permission;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class HdfsPermisson {



   public static void main(String[] args) throws IOException {
       Configuration configuration=new Configuration();
       configuration.set("fs.default.name","hdfs://192.168.147.130:9000");
       FileSystem fileSystem=FileSystem.get(configuration);


   }

}
