package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");

            programDriver.addClass("arrondissement_count", ArrandisementCount.class,
                            "A map/reduce program that give the number of arrondissement.");
            programDriver.addClass("species_count", SpeciesCount.class,
                    "A map/reduce program that give all species tree.");
            programDriver.addClass("speciesCounteach_species", SpeciesCount.class,
                    "A map/reduce program that give the number of trees for each species.");
            programDriver.addClass("countTallTree", EachSpeciesAverage.class,
                    "A map/reduce program that give the tallest tree for each species.");
            programDriver.addClass("SmallestToLargest", SmallestToLargest.class,
                    "A map/reduce program that give the smallest to largest tree for all species.");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}