package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreesHeightMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    private boolean isHeader = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader) {
            isHeader = false;
            return; // Skip the header row
        }
        String[] fields = value.toString().split(";");
        if (fields.length > 6) { // Assuming height is the 7th field (index 6)
            try {
                double height = Double.parseDouble(fields[6]);
                context.write(new DoubleWritable(height), value);
            } catch (NumberFormatException e) {
                // Handle the case where the height field is not a valid number
            }
        }
    }
}
