package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer184 extends Reducer<Text, IntWritable, Text, IntWritable> {


    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int maxVal = 0;
        for (IntWritable val : values) {
            if (val.get() > maxVal) {
                maxVal = val.get();
            }
        }
        context.write(key, new IntWritable(maxVal));
    }
}