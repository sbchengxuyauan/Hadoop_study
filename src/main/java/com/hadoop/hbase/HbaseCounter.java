package com.hadoop.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

public class HbaseCounter {
    private static org.apache.hadoop.conf.Configuration conf;
    private static HTable table=null;

    /**
     * 单独作为一个表中的一行，可以用来计数
     * @throws IOException
     */
    public void singelcounnter() throws IOException {
        long i1=table.incrementColumnValue(Bytes.toBytes("20110102"),Bytes.toBytes("info"),Bytes.toBytes("address"),1);
        System.out.println(i1);
        long i2=table.incrementColumnValue(Bytes.toBytes("20110101"),Bytes.toBytes("info"),Bytes.toBytes("address"),20);
        System.out.println(i2);
    }

    /**
     * 多列计数器  一行存在多个计数器，计数器之间的区别不是以名字来区分的
     */
    public void morecounnter() throws IOException {
        Increment increment=new Increment(Bytes.toBytes("zym01"));
        increment.addColumn(Bytes.toBytes("info"),Bytes.toBytes("count"),1);
        increment.addColumn(Bytes.toBytes("info"),Bytes.toBytes("count"),20);
        increment.addColumn(Bytes.toBytes("info"),Bytes.toBytes("count"),30);
        increment.addColumn(Bytes.toBytes("info"),Bytes.toBytes("count"),40);
        Result res=table.increment(increment);
        for (Cell cell:res.rawCells()){
           System.out.println(Bytes.toLong(cell.getValue()));
        }
//        long i=table.incrementColumnValue(Bytes.toBytes("zym01"),Bytes.toBytes("info"),Bytes.toBytes("count"),0);
//        System.out.println(i);
    }


    public  static  void  main(String[] args) throws IOException{
        conf=HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");
        table=new HTable(conf,"zym02");
        HbaseCounter counter=new HbaseCounter();
       // counter.singelcounnter();
          counter.morecounnter();
    }

}
