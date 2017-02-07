package com.dataartisans.eventsession;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.XORShiftRandom;

import java.util.Random;

/**
 * Shuffler emitting events from a buffer in random order
 */
public class StreamShuffler<T> extends RichFlatMapFunction<T, T> {
    private final int windowSize;
    protected final Random RND = new XORShiftRandom();
    private transient T[] buffer;
    private transient int fillIndex;

    public StreamShuffler(int windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.buffer = (T[]) new Object[windowSize];
        this.fillIndex = 0;
    }

    @Override
    public void flatMap(T value, Collector<T> out) throws Exception {
        if(fillIndex < buffer.length) {
            // fill buffer
            buffer[fillIndex++] = value;
        } else {
            // buffer full. Randomly select element for emission.
            int replaceIndex = getNext(buffer.length);
            out.collect(buffer[replaceIndex]);
            buffer[replaceIndex] = value;
        }
    }

    public int getNext(int bound) {
        return RND.nextInt(bound);
    }
}
