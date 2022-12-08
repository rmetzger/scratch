package de.robertmetzger;

import org.apache.lucene.util.LongBitSet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DataValidator {

    public static void main(String[] args) throws IOException {
        LongBitSet bitset = new LongBitSet(150_000_000L);
        try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
            String line;
            while ((line = br.readLine()) != null) {
                long id = Long.parseLong(line.split(":")[1]);
                if(bitset.get(id)) {
                    throw new IllegalStateException("id " + id + " is set twice!");
                }
                bitset.set(id);
            }
        }
        // validate bitset
        for (long i = 0; i < LongBitSet.MAX_NUM_BITS; i++) {
            if (!bitset.get(i)) {
                System.out.println("First unset index is " + i);
                break;
            }
        }
    }
}
