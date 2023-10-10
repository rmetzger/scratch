package co.decodable.flink.example.iot.operators;

import org.apache.flink.api.common.functions.RichMapFunction;

import co.decodable.flink.example.iot.types.SubscriptionRule;

public class MapSubscriptionFromJSON extends RichMapFunction<String, SubscriptionRule> {

    @Override
    public SubscriptionRule map(String value) throws Exception {
        // TODO
        return null;
    }
}
