package com.opstty;

import com.opstty.job.DistrictsContainingTrees;
import com.opstty.job.WordCount;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            // programDriver.addClass("wordcount", WordCount.class,
            // "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtscount", DistrictsContainingTrees.class,
                    "A map/reduce program that counts the number of districts containing trees in the input files.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
