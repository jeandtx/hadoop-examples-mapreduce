package com.opstty;

import com.opstty.job.WordCount;
import org.apache.hadoop.util.ProgramDriver;
import com.opstty.job.DistrictsContainingTrees;
import com.opstty.job.ListSpecies;
import com.opstty.job.OldestTree;
import com.opstty.job.TallestTree;
import com.opstty.job.TreeKindCount;
import com.opstty.job.TreesHeightSort;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtscount", DistrictsContainingTrees.class,
                    "A map/reduce program that counts the number of districts containing trees in the input files.");
            programDriver.addClass("listspecies", ListSpecies.class,
                    "A map/reduce program that lists the different species in the input files.");
            programDriver.addClass("oldesttree", OldestTree.class,
                    "A map/reduce program that displays the oldest tree in the input files.");
            programDriver.addClass("tallesttree", TallestTree.class,
                    "A map/redcue program that displays the tallest tree in the input files.");
            programDriver.addClass("treekindcount", TreeKindCount.class,
                    "A map/reduce program that counts the number of trees of each kind in the input files.");
            programDriver.addClass("treesheightsort", TreesHeightSort.class,
                    "A map/reduce program that sorts the trees by height in the input files.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
