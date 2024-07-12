package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int minYear = Integer.MAX_VALUE;
        for (IntWritable val : values) {
            minYear = Math.min(minYear, val.get());
        }
        context.write(key, new IntWritable(minYear));
    }
}
