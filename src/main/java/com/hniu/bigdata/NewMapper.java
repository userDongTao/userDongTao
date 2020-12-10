package com.hniu.bigdata;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NewMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private static final Text word = new Text();
    private static final IntWritable one = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] str = line.split("，");
        for (String e: str) {
////            str.set();
//            context.write(new Text(e), new IntWrita ble(1));
            new Text(e);
        context.write(new Text(str[1]),one);

        }
    }
}