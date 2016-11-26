package com.dataartisans.streamutils;

import org.apache.flink.configuration.Configuration;


public class FlinkUtils {
    /**
     * You need the "flink-metrics-jmx" dependency to use this utility.
     *
     * @param configuration configuration to add the reporter to.
     */
    public static void enableJMX(Configuration configuration) {
        configuration.setString("metrics.reporters", "my_jmx_reporter");
        configuration.setString("metrics.reporter.my_jmx_reporter.class", "org.apache.flink.metrics.jmx.JMXReporter");
        configuration.setString("metrics.reporter.my_jmx_reporter.port", "9020-9040");
    }
}
