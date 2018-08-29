package com.hadoop.zym.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

import java.io.IOException;

public class TestFileSystem {


    public  static  void main(String[] args) throws IOException {
        Configuration configuration=new Configuration();
        configuration.set("fs.default.name","hdfs://192.168.147.130:9000");
        FileSystem fileSystem=FileSystem.get(configuration);

        //源码:
        /*FSNamesystem fsNamesystem
          FSDirectory dir; 存贮了整个HDFS文件系统的文件目录信息  实现元数据的加载，合并，内存合并，保存
          BlockManager blockmanager;存贮了Block的相关信息,包括Block与元数据信息的映射
          NavigableMap datanodeMap ;保存了所有的DataNode信息，每一个DataNode记录了当前可用的磁盘空间，最后的一次更新时间，维护了该DataNode所有的Block信息
                                    每一个DatanodeDescriptor代表了一个Datanode


        */









    }

}
