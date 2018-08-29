package com.hadoop.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Hbase
 */
public class Testhbase01 {
    private  static Configuration conf;
    private  static HTable table;

    public  static void  main(String[] args) throws IOException, InterruptedException {
             conf=HBaseConfiguration.create();
             conf.addResource("hbase-site.xml");
             table=new HTable(conf,"zym02");  //创建table
             Testhbase01 hbase=new Testhbase01();
            // hbase.put();
            // hbase.delete();
            // hbase.listput();
            // hbase.checkAndput();
            // hbase.batch();
               hbase.Scan();
          }


    /**
     * 添加数据
     */
    public void put() throws IOException {
        Put put=new Put(Bytes.toBytes("row1"));   //row-key值
        //不添加时间，其默认使用来自构造函数的时间戳
       /*
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("zhouyanmin")); //向列族发finfo中的列name添加值为zhouyanmin的数据
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes("20"));          //向列族发finfo中的列name添加值为zhouyanmin的数据
        put.addColumn(Bytes.toBytes("work"),Bytes.toBytes("work01"),Bytes.toBytes("java"));      //向列族发finfo中的列name添加值为zhouyanmin的数据
        put.addColumn(Bytes.toBytes("work"),Bytes.toBytes("work02"),Bytes.toBytes("C++")); //向列族发finfo中的列name添加值为zhouyanmin的数据
        */
        //指定时间，会自动分配单元格
        //put.addColumn(Bytes.toBytes("finfo"),Bytes.toBytes("address"),new Date().getTime(),Bytes.toBytes("shenzhen")); //指定时间
        //table.put(put);
//        NavigableMap<byte[], List<Cell>> map=put.getFamilyCellMap();
//        List<Cell> list=map.get(Bytes.toBytes("finfo"));
//        for(int i=0;i<list.size();i++){
//            System.out.println(list.get(i).getFamilyArray());
//        }
        ResultScanner scanner=table.getScanner(Bytes.toBytes("finfo"));
        Result result=null;
       for (result=scanner.next();result!=null;result=scanner.next()){
          // System.out.println(new String(result.getRow()));
           System.out.println(new String(result.value()));
        }

    }

    /**
     * 删除指定的键
     */
    public  void  delete() throws IOException {
        Delete delete=new Delete(Bytes.toBytes("row1"));  //删除指定内容
     table.delete(delete.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age")));
    }

    /**
     * 批量插入数据
     */
    public  void  listput() throws IOException {
        table.setAutoFlush(false,false);
        List<Put> puts=new ArrayList<>();
        for(int i=4;i<1000;i++){
           Put put=new Put(Bytes.toBytes("row"+i));
           put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("address"),Bytes.toBytes("sicuan"));
           puts.add(put);
        }
        table.put(puts);
        table.flushCommits();
    }

    /**
     * 原子性操作,检查写
     */
    public void checkAndput() throws IOException {
    Put put=new Put(Bytes.toBytes("id01"));
    table.checkAndPut(Bytes.toBytes("id01"),Bytes.toBytes("info"),Bytes.toBytes("address"),Bytes.toBytes("sicuan"),put);

    }


    /**
     * get方法
     *
     */
    public  void Get(){
     Get get=new Get(Bytes.toBytes("row1"));
    }

//    public void result(){
//     Put put=new Put(Bytes.toBytes("row1"));
//
//    }

    /**
     * 批量操作,可以将表的增删查改放在一起进行操作
     */
    public void batch() throws IOException, InterruptedException {
    List<Row> rows=new ArrayList<>();
//    for(int i=0;i<1000;i++){
//       Delete delete=new Delete(Bytes.toBytes("row"+i));
//       rows.add(delete);
//    }
//    Put put=new Put(Bytes.toBytes("zym01"));
//    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(20));
//    rows.add(put);
//
      Get get=new Get(Bytes.toBytes("zym01"));
      get.addFamily(Bytes.toBytes("info"));
//    rows.add(get);
//    Object[] results=new Object[rows.size()];
//    table.batch(rows,results);
//    for (int i=0;i<results.length;i++){
//       System.out.println(results[i]);
//    }
    Result result=table.get(get);
    System.out.println(result);
    }


    /**
     * 行锁
     *
     */
    public  void  lock(){
     Put put=new Put(Bytes.toBytes("zym01"));
    }

    /**
     * 扫描(与GET是不相同的)
     *
     */
    public void  Scan() throws IOException {
//     table.setAutoFlush(false,false);   //插入10000条数据
//     List<Put> puts=new ArrayList<>();
//     for(int i=1;i<10000;i++){
//       Put put=new Put(Bytes.toBytes("row"+i));
//       put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("c"+i),Bytes.toBytes(i));
//       puts.add(put);
//     }
//     table.put(puts);
//     table.flushCommits();

       Scan scan=new Scan(); //扫描指定区间row-key的值
       scan.addFamily(Bytes.toBytes("info"));// 添加列族
       ResultScanner res=table.getScanner(scan);
       Iterator<Result> ite=res.iterator();
       try {
       while (ite.hasNext()){
        Result r=ite.next();
        System.out.println(r);
       }
       res.close();
       }catch (Exception e){
        e.printStackTrace();
       }finally {
       res.close(); //必须关闭
       }
    }

    /**
     * 缓存与批量处理
     *
     */
    public void Cach(){

    }



































    public void AutoFlush() throws IOException {
        table.setAutoFlush(false);//开启客户端缓存
        table.flushCommits();  //显式刷新  将客户端的缓存发送到服务器上

    }













}
