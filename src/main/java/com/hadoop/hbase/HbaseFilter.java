package com.hadoop.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Hbase的过滤器
 */
public class HbaseFilter {
    //Hbase的过滤器的接口为Filter，可以自定义类继承该接口，过滤器在服务端进行过滤，尽量避免在客户端进行过滤，因为这样会影响性能
     private static org.apache.hadoop.conf.Configuration conf;
     private static HTable table=null;




    /**
     * row-key过滤器 正则表达式很重要
     */
    public void RowkeyFilter() throws IOException {
        Scan scan=new Scan();
        scan.addFamily(Bytes.toBytes("info"));
        Filter filter1=new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes("row1000"))); //匹配小于或等于设定的值,使用Byte.Compare（）比较当前的值与阀值(全值比较器),字典顺序
        Filter filter2=new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*row9999.*"));   //键值包含row999的 正则表达式(正则比较器)
        Filter filter3=new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("row2")); //键值包含子字符串 （子字符串比较器)
        scan.setFilter(filter3);
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                System.out.println(r);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res.close();
            table.close();
        }
        table.close();
    }


    /**
     * FamilyFilter过滤器(列族)
     *
     */
    public void  FamilyFilter() throws IOException {
        Scan scan= new Scan();
        Filter filter1=new FamilyFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes("info"))); //匹配小于或等于设定的值,使用Byte.Compare（）比较当前的值与阀值(全值比较器),字典顺序
        Filter filter2=new FamilyFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*info.*"));   //键值包含row999的 正则表达式(正则比较器)
        Filter filter3=new FamilyFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("inf")); //键值包含子字符串 （子字符串比较器)
        scan.setFilter(filter1);
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                System.out.println("返回结果:"+r);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res.close();
            table.close();
        }
        table.close();
    }

    /**
     *
     *QualifierFilter(列名过滤器)
     */
    public void Columnname() throws IOException {
        Scan scan= new Scan();
        Filter filter1=new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes("c9999"))); //匹配小于或等于设定的值,使用Byte.Compare（）比较当前的值与阀值(全值比较器),字典顺序
        Filter filter2=new QualifierFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*c9999.*"));   //键值包含row999的 正则表达式(正则比较器)
        Filter filter3=new QualifierFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("c909")); //键值包含子字符串 （子字符串比较器)
        scan.setFilter(filter2);
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                System.out.println("返回结果:"+r);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res.close();
            table.close();
        }
        table.close();
    }

    /**
     * 值过滤器 valueFilter
     */
    public  void  valueFilter() throws IOException {
        Scan scan= new Scan();
        Filter filter1=new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes(1))); //匹配小于或等于设定的值,使用Byte.Compare（）比较当前的值与阀值(全值比较器),字典顺序
        Filter filter2=new ValueFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*99.*"));   //键值包含row999的 正则表达式(正则比较器)
        Filter filter3=new ValueFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("9090")); //键值包含子字符串 （子字符串比较器)
        scan.setFilter(filter3);
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                System.out.println("返回结果:"+r);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res.close();
            table.close();
        }
        table.close();
    }




    /**
     * 参考列过滤器 (复杂过滤器) 使用多个值进行组合过滤
     */
     public  void  Dependent(){
    // Filter filter=new DependentColumnFilter();
     }


    /**
     * 专用过滤器
     * 单列值过滤器
     */
    public void singelFilter() throws IOException {
      Filter filter=new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("c9999"),CompareFilter.CompareOp.NOT_EQUAL,new BinaryComparator(Bytes.toBytes(9999)));
      //辅助方法
      ((SingleColumnValueFilter) filter).setFilterIfMissing(false);   //返回的结果是否包含参考列
      ((SingleColumnValueFilter) filter).setLatestVersionOnly(false); //过滤器是否检查列的所有版本 默认为true,不检查
       Scan scan=new Scan();
       scan.setFilter(filter);
       ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                for (Cell kv:r.rawCells()){
                System.out.println("返回结果:"+new String(kv.getRow()));
                System.out.println("返回结果:"+Bytes.toInt(kv.getValue())); //字节数组转int
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res.close();
            table.close();
        }
        table.close();
    }

    /**
     * 前缀过滤器
     *
     */
    public void  PrefixFilter() throws IOException {
        Scan scan= new Scan();
        Filter filter1=new PrefixFilter(Bytes.toBytes("row999")); //匹配小于或等于设定的值,使用Byte.Compare（）比较当前的值与阀值(全值比较器),字典顺序
        scan.setFilter(filter1);
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                System.out.println("返回结果:"+r);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res.close();
            table.close();
        }
        table.close();
    }

    /**
     * 分页过滤器 需要处理第一行的键值
     *
     */
    public void  pageFilter() throws IOException {
        int K=0;
        int sum=0;
        byte[] lastrow=null;
        Scan scan= new Scan();
        Filter filter1=new PageFilter(1001);
        scan.setFilter(filter1);
//        ResultScanner res=table.getScanner(scan);
//        Iterator<Result> iter=res.iterator();
//        try {
//            while (iter.hasNext()) {
//                K=K+1;
//                Result r = iter.next();
//                System.out.println("返回结果:"+r);
//            }
//            System.out.println(K);
//        }catch (Exception e){
//            e.printStackTrace();
//        }finally {
//            res.close();
//            table.close();
//        }
//        table.close();
        while (true){
        if(sum>=10000){
          break;
        }
        if(lastrow!=null){
         scan.setStartRow(lastrow);
        }
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> ier=res.iterator();
        while (ier.hasNext()){
        Result r=ier.next();
        K++;
        if(K==1001){
        lastrow=r.getRow();
        System.out.println("************************"+K);
        K=0;
        continue;
        }
        System.out.println(r);
        sum=sum+1;
        }
        }

    }

    /**
     * 行键过滤器
     */
    public void keyonly(){
    Filter filter=new KeyOnlyFilter();
    }

    /**
     *首次行键过滤器
     *只访问一行的第一列,则可以使用这种过滤器
     */
    public void Firstkey(){
    Filter filter=new FirstKeyOnlyFilter();
    }

    /**
     *包含结束的过滤器
     */
    public  void overend() throws IOException {
    Scan scan=new Scan();
    Filter filter=new InclusiveStopFilter(Bytes.toBytes("row2000"));
    scan.setFilter(filter);
    table.getScanner(scan);
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                System.out.println("返回结果:"+r);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res.close();
            table.close();
        }
        table.close();
    }

    /**
     * 时间戳过滤器
     */
    public void  DateFilter() throws IOException {
        Scan scan=new Scan();
        List<Long> longs=new ArrayList<>();
        longs.add(new Long(50));
        longs.add(new Long(40));
        Filter filter=new TimestampsFilter(longs);
        scan.setFilter(filter);
        table.getScanner(scan);
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                System.out.println("返回结果:"+r);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res.close();
            table.close();
        }
        table.close();
    }

    /**
     *列计数过滤器 适合用于get 限制每一行取回多少列
     */
    public  void  ColumnFilter() throws IOException {
        Scan scan=new Scan();
        List<Long> longs=new ArrayList<>();
        Filter filter=new ColumnCountGetFilter(0); //列计数
        scan.setFilter(filter);
        table.getScanner(scan);
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                System.out.println("返回结果:"+r);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res.close();
            table.close();
        }
        table.close();
    }

    /**
     *列分页过滤器 对每一行返回的列进行分页
     */
    public void  Columnpage() throws IOException {
        Scan scan=new Scan();
        List<Long> longs=new ArrayList<>();
        Filter filter=new ColumnPaginationFilter(1,1); // 参数:limit offset
        scan.setFilter(filter);
        table.getScanner(scan);
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                System.out.println("返回结果:"+r);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res.close();
            table.close();
        }
        table.close();
    }

    /**
     *列前缀过滤器
     */
    public void  Columnpre() throws IOException {
        Scan scan=new Scan();
        List<Long> longs=new ArrayList<>();
        Filter filter=new ColumnPrefixFilter(Bytes.toBytes("c999"));
        scan.setFilter(filter);
        table.getScanner(scan);
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                System.out.println("返回结果:"+r);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res.close();
            table.close();
        }
        table.close();
    }

    /**
     *随机行过滤器,随机返回行数
     */
    public void radom() throws IOException {
        Scan scan=new Scan();
        List<Long> longs=new ArrayList<>();
        Filter filter=new RandomRowFilter((float) 0.0005);  //取值在0-1.0之间 不清楚其内部的机制 返回的结果很奇怪
        scan.setFilter(filter);
        table.getScanner(scan);
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                System.out.println("返回结果:"+r);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res.close();
            table.close();
        }
        table.close();
    }

    /**
     * 跳转过滤器,需要用户自定义一个过滤器，且自定义过滤器需要实现
     */
    public void myFilter() throws IOException {
        Scan scan=new Scan();
        List<Long> longs=new ArrayList<>();
        Filter filter=new PrefixFilter(Bytes.toBytes("row999"));
        scan.setFilter(filter);
        table.getScanner(scan);
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                System.out.println("返回结果:"+r);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        res.close();
        Filter filter1=new SkipFilter(filter);  //从整行扩展到整列
        scan.setFilter(filter1);
        ResultScanner res1=table.getScanner(scan);
        Iterator<Result> iter1=res.iterator();
        try {
            while (iter1.hasNext()) {
                Result r = iter1.next();
                System.out.println("返回结果:"+r);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res1.close();
            table.close();
        }
    }


    /**
     *全匹配过滤器(包装过滤器，组合成新的过滤器) 外层过滤行,内层过滤值
     */
    public void  allpre() throws IOException {
        Scan scan=new Scan();
        List<Long> longs=new ArrayList<>();
        Filter filter=new PrefixFilter(Bytes.toBytes("row999"));
        scan.setFilter(filter);
        table.getScanner(scan);
        ResultScanner res=table.getScanner(scan);
        Iterator<Result> iter=res.iterator();
        try {
            while (iter.hasNext()) {
                Result r = iter.next();
                System.out.println("返回结果:"+r);
            }
            System.out.println("******************************");
        }catch (Exception e){
            e.printStackTrace();
        }
        res.close();
        Filter filter1=new WhileMatchFilter(new ColumnPrefixFilter(Bytes.toBytes(9999)));  //从整行扩展到整列
        scan.setFilter(filter1);
        ResultScanner res1=table.getScanner(scan);
        Iterator<Result> iter1=res.iterator();
        try {
            while (iter1.hasNext()) {
                Result r = iter1.next();
                System.out.println("返回结果:"+r);
            }
            System.out.println("******************************");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            res1.close();
            table.close();
        }
    }




    public static  void main(String[] args) throws IOException {
        conf=HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");
        table=new HTable(conf,"zym02");
        HbaseFilter hbaseFilter=new HbaseFilter();
        // hbaseFilter.RowkeyFilter();//rowkey比较器
        // hbaseFilter.FamilyFilter();//列族比较器
        // hbaseFilter.Columnname();  //列名过滤器
        // hbaseFilter.valueFilter(); //值过滤器
        // hbaseFilter.singelFilter();//单列值过滤器
        // hbaseFilter.PrefixFilter();//前缀过滤器
        // hbaseFilter.pageFilter();  //分页过滤器
        // hbaseFilter.overend();     //结束过滤器
        // hbaseFilter.DateFilter();  //时间戳过滤器
        // hbaseFilter.ColumnFilter();//列计数器
        // hbaseFilter.Columnpage();  //列分页过滤器
        // hbaseFilter.Columnpre();   //列前缀过滤器
        // hbaseFilter.radom();       //随机行过滤
        // hbaseFilter.myFilter();    //跳转过滤器
           hbaseFilter.allpre();
    }

}


