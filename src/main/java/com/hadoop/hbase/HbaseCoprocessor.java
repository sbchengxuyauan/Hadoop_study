package com.hadoop.hbase;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 * 协处理器
 */
public class HbaseCoprocessor {
    private static org.apache.hadoop.conf.Configuration conf;
    private static HTable table=null;

   public static  void  main(String[] args) throws IOException {
   conf=HBaseConfiguration.create();
   conf.addResource("hbase-site.xml");
   table=new HTable(conf,"zym02");
   }

    /**一、协处理器类的基础:
     * 1、Cprocessor类，所有的协处理类都必须实现这个接口,其定义了协处理器的基本约定 比如说执行的优先级,提供了两个方法
     * void start(CoprocessorEnvironment env)  协处理器开始时被调用
     * void stop (CoprocessorEnvironment env)  协处理器结束时被调用
     *
     *2、CoprocessorEnvironment在协处理器的生命周期中保持其状态，其方法见笔记
     *3、CoprocessorHost维护协处理器实例和它们专用的环境
     *
     *
     *二、协处理器的加载
     *可以使用静态的方式进行加载，也可以是使用在集群中动态的进行加载（使用配置文件和表模式是静态加载方法）
     *在hbase-site.xml中配置需要加载的协助器，其存放的位置十分重要，其会按照存放的顺序进行加载
     *
     *
     * 三、处理region生命周期事件
     * 在一个region打开或者关闭前后执行函数
     * void preopen / void postopen
     *
     * void preclose /void postclose
     *
     * 四、处理客户端处理事件
     * 1、主要针对客户端对数据的CRUD操作
     * void preGet / void postGet    获取之前之后
     * void prePut / void postPut    插入之前之后
     * void preDelete / void postpreDelete 删除之前之后
     * void preCheckAndPut / void postCheckAndPut 原子性插入之前之后
     *
     * 2、维护其环境的ReginonCoprocessorEnvironment
     * 针对子类的方法
     * HRregion getRegion() 返回监听器监听的region的引用
     * RegionServerServices         getRegionServerServices() 返回共享的RegionServerServices的实例
     *
     * 3、上下文共享的变量ObserverContext类
     * 重要的方法：
     * bypass()
     * complete()
     *
     * 5、BaseRegionObserver类实现自己的协处理器可以继承此类，修改自己想要的方法
     *
     *
     */



    /**
     * 回调函数
     * RegionObserver  处理表数据处理事件 regionserver  数据操作
     * MasterObserver  管理和操作DDL类型的操作          表操作
     * WALObserver     控制WAL的子函数
     */
   public void observer(){

   }

    /**
     * 服务器端计算,可以在服务器端进行计算的操作
     *
     */
    public void endpoint(){

    }


}
