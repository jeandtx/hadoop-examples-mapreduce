package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TallestTreeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private boolean isHeader = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader) {
            isHeader = false;
            return; // Skip the header row
        }
        String[] fields = value.toString().split(";");
        String species = fields[3]; // Assuming the species is the fourth field
        double height;
        try {
            height = Double.parseDouble(fields[6]); // Assuming the height is the seventh field
        } catch (NumberFormatException e) {
            return; // Skip if height is not a valid number
        }
        context.write(new Text(species), new DoubleWritable(height));
    }
}
