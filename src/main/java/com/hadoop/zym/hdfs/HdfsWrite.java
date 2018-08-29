package com.hadoop.zym.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;


public class HdfsWrite {

    public static  void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
        ConnectionHdfs hdfs=new ConnectionHdfs();
        String path="D:\\Personal\\Desktop\\da\\delete.txt";
        HdfsWrite write=new HdfsWrite();
        //write.TestWrite(path,conf);
        //write.Testread(path,conf);
        write.delete(path,conf);
    }


    /**
     * 写入d
     */
    public void TestWrite(String path,Configuration configuration) throws IOException {

        Path path1=new Path(path);
        configuration.set("fs.default.name","hdfs://192.168.147.130:9000"); //配置文件系统
        FileSystem fileSystem=FileSystem.get(configuration);
       // byte[] buf=new byte[1024];

        FSDataOutputStream out=fileSystem.create(path1);
        BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(out));
        //FSDataOutputStream out=fileSystem.append(path1);
        //System.out.println(("华云中盛科技有限公司华云中盛科技有限公司华云中盛科技有限公司华云中盛科技有限公司华云中盛科技有限公司华云中盛科技有限公司华云中盛科技有限公司").getBytes("utf-8").length);
        out.write(("华云中盛科技有限公司华云中盛科技有限公司华云中盛科技有限公司华云中盛科技有限公司华云中盛科技有限公司华云中盛科技有限公司华云中盛科技有限公司").getBytes("GBK"));
//        for(int i=0;i<10;i++){
//        writer.write("华云中盛科技有限公司\n");
//        writer.flush();
//        }
        writer.close();

        //out.write();
        out.flush();
        out.close();
    }

    /**
     * 读取
     * @param path
     * @param configuration
     * @throws IOException
     */
    public void Testread(String path,Configuration configuration) throws IOException {
        Path path1=new Path(path);
        configuration.set("fs.default.name","hdfs://192.168.147.130:9000"); //配置文件系统
        FileSystem fileSystem=FileSystem.get(configuration);
        FSDataInputStream in=fileSystem.open(path1);

        BufferedReader reader=new BufferedReader(new InputStreamReader(in)); //行读取


        byte[] buf=new byte[30];
        byte[] buf1=new byte[2048];
        int position=0;
        int len=0;
        String str=null;
//        while ((str=reader.readLine())!=null){
//            System.out.println(str);
//        }
//        in.close();
//        reader.close();

//        while ((len=in.read(buf1))!=-1){
//            System.out.println("字节的长度:"+len+new String (buf1,"utf-8"));
//        }

//        in.read(position,buf1,0,10);
//        System.out.println("1111111111111111111");
//                    System.out.println(new String(buf1,"utf-8"));
//
//        in.read(position,buf1,2,10);
//
//        System.out.println(new String(buf1,"utf-8"));

//         in.read(position,buf1,32,100);

//        System.out.println(new String(buf1,"utf-8"));
//        华云中盛科技有限公司dajiddaidjiadidkiadiadnaidniadadadad大户大嘟嘟哈太



//        while ((len=in.read(position,buf,0,30))!=-1){   //相对于文件偏移的读取
//            position=len+position;
//            System.out.println(new String(buf,"utf-8"));
//        }
        in.close();

    }

    public void delete(String path,Configuration configuration) throws IOException {
        Path path1=new Path(path);
        configuration.set("fs.default.name","hdfs://192.168.147.130:9000"); //配置文件系统
//        FileSystem fileSystem=FileSystem.get(configuration);
//        File file=new File(path);
//        file.delete();
//        fileSystem.delete(path1,true);
        LocalFileSystem local=FileSystem.getLocal(configuration);
        local.delete(path1,true);


    }




}
