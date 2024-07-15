package com.opstty.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.opstty.mapper.DistrictsContainingTreesMapper;
import com.opstty.reducer.DistrictsContainingTreesReducer;
import com.opstty.mapper.DistrictWithMostTreesMapper;
import com.opstty.reducer.DistrictWithMostTreesReducer;

public class MostTreesDistrict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: listspecies <in> <out>");
            System.exit(2);
        }
        // Job 1: List all districts and number of trees
        Job job1 = Job.getInstance(conf, "DistrictsContainingTrees");
        job1.setJarByClass(MostTreesDistrict.class);
        job1.setMapperClass(DistrictsContainingTreesMapper.class);
        job1.setCombinerClass(DistrictsContainingTreesReducer.class);
        job1.setReducerClass(DistrictsContainingTreesReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        Path tempOutputPath = new Path("temp");
        FileOutputFormat.setOutputPath(job1, tempOutputPath);

        if (!job1.waitForCompletion(true)) {
            System.exit(1); // Exit if the job fails
        }

        // Job 2: Find the district with the most trees
        Job job2 = Job.getInstance(conf, "DistrictWithMostTrees");
        job2.setJarByClass(MostTreesDistrict.class);
        job2.setMapperClass(DistrictWithMostTreesMapper.class);
        job2.setReducerClass(DistrictWithMostTreesReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, tempOutputPath); // Input from Job 1's output
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

        if (!job2.waitForCompletion(true)) {
            System.exit(1); // Exit if the job fails
        }

    }
}
