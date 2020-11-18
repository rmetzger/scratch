package de.robertmetzger;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.XORShiftRandom;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class PageRankDataGen {

    public static void main(String[] args) throws IOException {
        ParameterTool pt = ParameterTool.fromArgs(args);
        long pages = pt.getLong("pages");
        Random RNG = new XORShiftRandom();
        try (BufferedWriter edgeOut = new BufferedWriter(new FileWriter("edges.txt"))) {
            try (BufferedWriter pagesOut = new BufferedWriter(new FileWriter("pages.txt"));) {
                for (long page = 0; page < pages; page++) {
                    // populate pages list
                    pagesOut.write(Long.toString(page));
                    pagesOut.write('\n');

                    int density = 1;
                    // build one long chain
                    if (page < 50_000L) {
                        writeEdge(page, page + 1, edgeOut);
                    }
                    if (page < 500) {
                        density = Math.min((int)pages, 2000);
                    }
                    for (int i = 0; i < density; i++) {
                        long nextPage = Math.abs(RNG.nextLong()) % pages;
                        writeEdge(page, nextPage, edgeOut);
                    }
                }
            }
        }
    }

    public static void writeEdge(long from, long to, BufferedWriter out) throws IOException {
        StringBuilder sb = new StringBuilder();

        sb.append(from);
        sb.append(' ');
        sb.append(to);
        sb.append("\n");

        out.write(sb.toString());
    }
}
