package com.haishui;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author haishui
 */
public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.execute();
    }
}
