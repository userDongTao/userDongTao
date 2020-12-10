package com.hniu.bigdata;

import org.apache.hadoop.hdfs.server.namenode.Content;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class NewReducer extends Reducer<Text, IntWritable, Text,IntWritable>{
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
        int sum= 0 ;
        for(IntWritable num:values){
            sum+=num.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
