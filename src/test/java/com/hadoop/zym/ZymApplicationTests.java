package com.hadoop.zym;

import com.hadoop.zym.mapreduce.MyReducer;
import com.hadoop.zym.mapreduce.Mymapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Arrays;


@RunWith(SpringRunner.class)
@SpringBootTest
public class ZymApplicationTests {

    @Test
    public void contextLoads() {
    }

    /**
     *
     * @throws
     */
    @Test
    public void testmapper() throws IOException {
     Mymapper mymapper=new Mymapper();
     MapDriver driver=new MapDriver();
     driver.withMapper(mymapper);
     String line="1995xxx25";
     driver.withInput(new LongWritable(1995),new Text(line));
     driver.withOutput(new Text("1995"),new IntWritable(25));
     driver.run();
    }

    @Test
    public void testreduce() throws IOException {
        MyReducer reducer=new MyReducer();
        ReduceDriver reduceDriver=new ReduceDriver();
        reduceDriver.withInput(new Text("1995"), Arrays.asList(new IntWritable(10),new IntWritable(5),new IntWritable(20),new IntWritable(9),new IntWritable(1)));
        reduceDriver.run();
    }


    public Text gettext(){
        Text text=new Text();
        StringBuilder builder=new StringBuilder();
        int year= (int) ((Math.random()*9999)-1000);
        builder.append(year);
        System.out.println(year);
        int ass=((int) Math.random()*122)-97;
        for(int i=0;i<4;i++){
        char css=(char)ass;
        System.out.println(css);
        builder.append(css);
        }
        int k=0;
        for(int j=0;j<2;j++){
          k=(int)Math.random()*50;
        }
        text.set(builder.toString());
        return  text;
    }
}
