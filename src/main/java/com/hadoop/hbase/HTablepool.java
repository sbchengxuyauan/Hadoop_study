package com.hadoop.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

public class HTablepool {
    private static org.apache.hadoop.conf.Configuration conf;
    private static HTablePool pool;

    public static  void main(String[] args){
        conf=HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");
        pool=new HTablePool(conf,3);
    }
}
