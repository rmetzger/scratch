package com.dataartisans.utils;

import com.dataartisans.eventsession.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThroughputLogger implements FlatMapFunction<Event, Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(ThroughputLogger.class);

    private long totalReceived = 0;
    private long lastTotalReceived = 0;
    private long lastLogTimeMs = -1;
    private long logfreq;

    public ThroughputLogger(long logfreq) {
        this.logfreq = logfreq;
    }

    @Override
    public void flatMap(Event element, Collector<Integer> collector) throws Exception {
        totalReceived++;
        if (totalReceived % logfreq == 0) {
            // throughput over entire time
            long now = System.currentTimeMillis();

            // throughput for the last "logfreq" elements
            if(lastLogTimeMs == -1) {
                // init (the first)
                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            } else {
                long timeDiff = now - lastLogTimeMs;
                long elementDiff = totalReceived - lastTotalReceived;
                double ex = (1000/(double)timeDiff);
                LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core. {} MB/sec/core. GB received {}",
                        timeDiff, elementDiff, elementDiff*ex, elementDiff*ex*15 / 1024 / 1024, (totalReceived * 15) / 1024 / 1024 / 1024);
                // reinit
                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            }
        }
    }
}