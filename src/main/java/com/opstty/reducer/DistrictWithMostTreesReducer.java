package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictWithMostTreesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Text maxDistrict = new Text();
    private int maxCount = 0;

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        if (sum > maxCount) {
            maxCount = sum;
            maxDistrict.set(key);
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(maxDistrict, new IntWritable(maxCount));
    }
}
