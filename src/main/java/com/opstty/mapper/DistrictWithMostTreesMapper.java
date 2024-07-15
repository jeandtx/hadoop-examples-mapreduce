package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictWithMostTreesMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String district = fields[0];
        int treeCount = Integer.parseInt(fields[1]);
        context.write(new Text(district), new IntWritable(treeCount));
    }
}
