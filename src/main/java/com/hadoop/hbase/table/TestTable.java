package com.hadoop.hbase.table;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class TestTable {
    private static org.apache.hadoop.conf.Configuration conf;
    private static HTable table=null;
    private static HBaseAdmin admin;



    public void TableandColum(){
      byte[] tablename= Bytes.toBytes("zym03");
      HTableDescriptor table=new HTableDescriptor(tablename);
      //表
      /*设置一个表中最多可以多少个region,Hfile文件会被分配到不同reginon服务器中，一个列族代表一个Hfile文件,当一个列族的数据列存在过多时，
        就会自动分离,到不同的region上,一行数据不会跨行存储，如果行数据太大的话是不会拆分region的
      */
      table.setMaxFileSize(10);
      table.getMaxFileSize();
      table.isReadOnly();  //设置表是否只读
      table.setMemStoreFlushSize(256); //设置存在有多少数据时就将数据写入到磁盘当中
      table.getMemStoreFlushSize();




      //列族
      HColumnDescriptor column=new HColumnDescriptor(Bytes.toBytes("info"));
      column.setMaxVersions(5);  //设置最大版本数
      column.getMaxVersions();
      column.setCompactionCompressionType(Compression.Algorithm.GZ); //设置数据压缩类型
      column.getCompactionCompression();   //获取压缩类型
      column.setBlocksize(128); //设置存储文件被拆分的块大小，默认为64KB,在hbase读取过程中读取多少数据到内存缓冲区当中去
      column.getBlocksize();
      column.setBlockCacheEnabled(false);//设置内存缓存块,读取相邻的数据时，就不会再次到磁盘中读取 默认为true
      column.isBlockCacheEnabled();
      column.setTimeToLive(12800000);//设置数据版本存在的上限时间,默认为Integer.MAX_VALUE 2147483647 默认值可以理解为永久保留
      column.setInMemory(true);  //将列族的数据块全部加载到内存当中，适合小数据量的列族,如果其数据量的值大于堆的上限值，那么数据就会被强制移除,默认为false
      column.isInMemory();
      column.setBloomFilterType(BloomType.NONE);  //布隆过滤器,默认为关闭，不使用
      column.getBloomFilterType();
      column.setScope(0); //高级复制 默认为0关闭 1为开启
      column.getScope();
      HColumnDescriptor.isLegalFamilyName(Bytes.toBytes("info")); //判断列族是否存在 静态方法
      table.addFamily(column);  //添加列族
    }

     public void HbaseAdmin() throws IOException {
        HBaseAdmin admin=new HBaseAdmin(conf);

        admin.isMasterRunning(); //检查master是否在运行
        admin.getConnection();   //返回连接
        admin.getConfiguration();//获取配置信息

        HTableDescriptor table=new HTableDescriptor("zym03");
        HColumnDescriptor column=new HColumnDescriptor(Bytes.toBytes("info"));
        table.addFamily(column);
        /**
         * 表操作
         */
        admin.createTable(table); //创建表

    }

    /**
     * 预分区建表
     */
    public void createTabel() throws IOException {
        HBaseAdmin admin=new HBaseAdmin(conf);
        HTableDescriptor mytable=new HTableDescriptor("zym05");
        HColumnDescriptor column=new HColumnDescriptor(Bytes.toBytes("info"));
        mytable.addFamily(column);
//        for(int i=1;i<=50;i++){
//           int K= (int) (Math.random()*10)+1;
//           System.out.println(K);
//        }
        int[] keys=new int[]{1000,2000,3000,4000,5000,6000,7000,8000,9000};
        byte[] [] bytes=new byte[keys.length][]; //分区数组边界
        TreeSet<byte[]> set=new TreeSet<>(Bytes.BYTES_COMPARATOR);  //利用Treeset进行排序,不排序会出错
        for(int i=0;i<keys.length;i++){
            set.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> ier=set.iterator();
        int j=0;
        while (ier.hasNext()){
           bytes[j]=ier.next();
           ier.remove();
           j++;
        }
        admin.createTable(mytable,bytes);   //bytes必须是有序的
    }

    public void PutData() throws IOException {
     HTable table=new HTable(conf,"zym05");
     table.setAutoFlush(false,false);
     List<Put> puts=new ArrayList<>();
     for(int i=1;i<=10000;i++){
         int K= (int) (Math.random()*10000+1);
         Put put=new Put(Bytes.toBytes(Integer.valueOf(K).toString()));
         put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("c"+Integer.valueOf(K).toString()),Bytes.toBytes(Integer.valueOf(K).toString()));
         puts.add(put);
     }
     table.put(puts);
     table.flushCommits();
    }

    /**
     * 查看分区信息
     * @throws IOException
     */
    public void getRegion() throws IOException {   //一个表被分成多个region那么其第一个region的startkey的第一个值为空 endkey的最后一个列也为空
     HTable table=new HTable(conf,"zym05");
     RegionLocator regions=table.getRegionLocator();
     byte[][] start=regions.getStartKeys();
     byte[][] end=regions.getEndKeys();
     for(int i=0;i<start.length;i++){
        if(start[i].length==0)
          continue;
        System.out.println(start[i].length);
        System.out.println(Bytes.toInt(start[i],0,4));
     }
     System.out.println("分割线****************************");
     for(int j=0;j<end.length;j++){
        if(end[j].length==0)
           continue;
        System.out.println(end[j].length);
        System.out.println(Bytes.toInt(end[j],0,4));
     }


//     List<HRegionLocation> list=regions.getAllRegionLocations();
//     for(int i=0;i<list.size();i++){
//        System.out.println(list.get(i).getRegionInfo());
//        System.out.println(list.get(i).getServerName());
//        System.out.println(list.get(i).getPort());
//     }

    }

    /**
     * 删除表
     * @throws IOException
     */
    public void deletetable() throws IOException {
     HTable table=new HTable(conf,"student");
        Delete delete=new Delete(Bytes.toBytes("info"));
        table.delete(delete);
    }

    /**
     * 查看表结构
     * @throws IOException
     */
    public void desctable() throws IOException {
    HBaseAdmin admin=new HBaseAdmin(conf);
    HTableDescriptor[] tables=admin.listTables();
    for(int i=0;i<tables.length;i++){
         System.out.println(tables[i]);
//       System.out.println(tables[i].getTableName().toString());
//       System.out.println(tables[i].getTableName().getNamespace());
//       System.out.println(new String(tables[i].getTableName().getQualifier()));
       System.out.println("************************");
    }
    }


    /**
     * 修改表
     */
    public void modifytable() throws IOException {
     HTableDescriptor table=admin.getTableDescriptor(Bytes.toBytes("zym03"));
     HTable table1=new HTable(conf,"zym03");
     admin.modifyTable("zym02",table);
    }


    public static  void main(String[] args) throws IOException {
        conf=HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");
        admin=new HBaseAdmin(conf);
        //admin.getConnection();
        //table=new HTable(conf,"zym02");
        TestTable table=new TestTable();
        //table.createTabel();  //创建表
        //table.PutData();      //写入数据
//        table.getRegion();    //获取分区信息
//        Boolean boo=admin.isTableDisabled("TABLE"); //查看表是否禁用
//        System.out.println(boo);
//        if(boo==false){
//           admin.disableTable("TABLE");             //禁用表
//        }
//        admin.deleteTable(Bytes.toBytes("TABLE"));  //删除表必须要禁用才能删除
//        table.deletetable();
//        table.desctable();   //获取表列表
          admin.enableTable("zym03"); //启用表，表如果没被禁用则会报异常
          admin.close();

    }

}
