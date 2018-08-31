package com.hadoop.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义过滤器，实现Filter接口或者继承FilterBase类,需要配置到服务器当中
 */
public  class MyFilter extends FilterBase {
    private byte[] value=null;
    private boolean filterRow=true;

    public MyFilter(){
       super();
    }

    public  MyFilter(byte[] bytes){ //设置要比较的值
        this.value=bytes;
    }

    @Override
    public void reset(){
        this.filterRow=true;    //当有新行时重置过滤器的标志位
    }



    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException { //当传入值与设定的值匹配时，就通过这一行
        if((Bytes.compareTo(value,cell.getValue()))==0){
         filterRow=false;
        }
        return ReturnCode.INCLUDE; //值包含在结果中
    }

    @Override
    public boolean filterRow(){   //决定数据是否被返回
       return filterRow;
    }


    public void write(DataOutput dataOutput) throws IOException {  //将值写入到输出流当中,使服务器可以读取到这一行数据
        Bytes.writeByteArray(dataOutput,this.value);
    }


    public void readFields(DataInput dataInput) throws IOException { //服务器端使用来初始化过滤器实例，客户端的值可以被读取到
        this.value=Bytes.readByteArray(dataInput);
    }

}
