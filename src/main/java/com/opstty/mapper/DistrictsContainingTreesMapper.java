package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictsContainingTreesMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private boolean isHeader = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader) {
            isHeader = false;
            return; // Skip the header row
        }
        String[] fields = value.toString().split(";");
        String district = fields[1]; // Assuming the district is the second field
        context.write(new Text(district), one);
    }
}
