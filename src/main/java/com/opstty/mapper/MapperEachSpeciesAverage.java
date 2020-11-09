package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapperEachSpeciesAverage extends Mapper<Object, Text, Text, IntWritable> {

    private Text species = new Text();
    private Text values = new Text();


    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");


        while (itr.hasMoreTokens()){
            values.set(itr.nextToken());
            species.set(itr.nextToken());
            if(species.toString().contains(";")) {
                Text arrNum = new Text(species.toString().split(";")[3]);
                String valNum = values.toString().split(";")[7];


                if ((!arrNum.equals(new Text("ESPECE"))
                        && !valNum.equals(new Text("HAUTEUR")))) {

                    context.write(arrNum, new IntWritable((int)Double.parseDouble(valNum)));
                    //System.out.println(arrNum.toString());
                }
            }
        }
    }
}
