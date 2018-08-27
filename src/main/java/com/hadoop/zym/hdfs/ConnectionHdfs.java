package com.hadoop.zym.hdfs;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;


public class ConnectionHdfs extends Configured {


    public static  void  main(String[] args) throws IOException {

        Configuration conf=new Configuration();
        ConnectionHdfs hdfs=new ConnectionHdfs();
        hdfs.writeString(conf,"/test/writestring.txt",10);
//        conf.set("fs.default.name","hdfs://192.168.147.130:9000"); //连接
//        FileSystem fileSystem=FileSystem.get(conf);  //获取对象
         //FSDataOutputStreamBuilder out=fileSystem.createFile(new Path("/test/zym"));
        //Boolean b=fileSystem.mkdirs(new Path("/test/zym"));//创建文件夹
        //FSDataOutputStreamBuilder out=fileSystem.createFile(new Path("/test/wordcount.txt"));
        //FSDataOutputStreamBuilder fileout=out.append();
//        FSDataOutputStream dataout=fileSystem.create(new Path("/test/write.txt"),false);//获取文件的输出流
//        for(int i=0;i<3;i++){
//             dataout.writeInt(1995+i);  //其写入的形式是字节流,序列化的形式写入
//        }
//        dataout.close();
//         FSDataInputStream filein=fileSystem.open(new Path("/test/write.txt"));
//        byte[] bytes=new byte[4];
//        int len=0;
//        filein.readInt();
//        try {
//        while (filein.available()>=4){
//           len=filein.readInt();
//           System.out.println(len);
//        }
//        }catch (IOException e){
//            e.printStackTrace();
//            filein.close();
//        }
//        finally {
//            if(filein!=null){
//                filein.close();
//            }
//        }

    }

    /**
     * 写入string
     * @param configuration
     * @param file
     * @param k
     */

    public void writeString(Configuration configuration,String file,int k){
        Path path=new Path(file);
        configuration.set("fs.default.name","hdfs://192.168.147.130:9000");
        try {
            FileSystem fileSystem=FileSystem.get(configuration);
            FSDataOutputStream out=fileSystem.create(path,false);
            for(int i=0;i<k;i++){
                out.writeChars("第"+(i+1)+"条字符");
                out.writeChars("&&");
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void readString(Configuration configuration,String file){
        Path path=new Path(file);
        configuration.set("fs.default.name","hdfs://192.168.147.130:9000");
        try {
            FileSystem fileSystem=FileSystem.get(configuration);
            FSDataInputStream in=fileSystem.open(path);
            int len=new String("&&").getBytes().length;
            byte[] str=new String("&&").getBytes();
            byte[] bytes=new byte[len];    //分割符字节数组的长度
            byte[] newbyte=new byte[0];   //新建的字节数组
            int strlength=0;
            System.out.println("&&字符串的长度为:"+len);
            while (in.available()>=len){
                if(Arrays.equals(bytes,str)==false){//比较两个字节数组是否相同
                    System.out.println("字节长度"+bytes.length);
                    byte[] oldbyte=newbyte;
                    newbyte =new byte[bytes.length+strlength];  //新增的字节数组
                    System.arraycopy(oldbyte,0,newbyte,strlength,oldbyte.length);  //合并新老字符串
                    strlength=newbyte.length;
                    System.arraycopy(bytes,0,newbyte,strlength,bytes.length);
                }
                System.out.println(new String(bytes));
                in.read(bytes);
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
