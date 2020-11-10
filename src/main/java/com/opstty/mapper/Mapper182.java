package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Mapper182 extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text species = new Text();


    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");


        while (itr.hasMoreTokens()){
            species.set(itr.nextToken());
            if(species.toString().contains(";")) {
                Text arrNum = new Text(species.toString().split(";")[3]);


                if (!arrNum.equals(new Text("ESPECE"))) {

                    context.write(arrNum, one);
                    //System.out.println(arrNum.toString());
                }
            }
        }
    }
}