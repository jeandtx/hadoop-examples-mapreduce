package com.opstty.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<Object, Text, Text, Text> {
    private boolean isHeader = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader) {
            isHeader = false;
            return; // Skip the header row
        }
        String[] fields = value.toString().split(";");
        if (fields.length > 3) { // Ensure that the row has enough columns
            String species = fields[3]; // ESPECE is the fourth field
            context.write(new Text(species), new Text("")); // Use species as the key, empty string as the value
        }
    }
}
