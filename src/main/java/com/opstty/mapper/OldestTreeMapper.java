package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<Object, Text, Text, IntWritable> {
    private boolean isHeader = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader) {
            isHeader = false;
            return; // Skip the header row
        }
        String[] fields = value.toString().split(";");
        String district = fields[1]; // Assuming the district is the second field
        String yearStr = fields[5]; // Assuming the year of plantation is the sixth field

        try {
            int year = Integer.parseInt(yearStr);
            context.write(new Text(district), new IntWritable(year));
        } catch (NumberFormatException e) {
            // Skip rows with invalid year
        }
    }
}
