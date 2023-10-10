package co.decodable.flink.example.iot;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import co.decodable.flink.example.iot.types.DeviceEvent;
import co.decodable.flink.example.iot.types.RuleAction;
import co.decodable.flink.example.iot.types.SubscriptionRule;

public class RuleEvaluator
        extends RichCoFlatMapFunction<DeviceEvent, SubscriptionRule, RuleAction> {

    // This state is managed by Flink: we periodically back up the contents of this fields and
    // restore it on error
    // it's size (across all customers) can grow well beyond the available heap. We'll store it on
    // disk.
    private ListState<SubscriptionRule> rules;

    @Override
    public void flatMap1(DeviceEvent value, Collector<RuleAction> out) throws Exception {
        // we received an update from a device, evaluate rules
        // access current rules to evaluate:
        for (SubscriptionRule rule : rules.get()) {
            // TODO
        }
    }

    @Override
    public void flatMap2(SubscriptionRule value, Collector<RuleAction> out) throws Exception {
        // we received an update to a subscription rule
        rules.add(value);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<SubscriptionRule> stateDescriptor =
                new ListStateDescriptor<>("rules", SubscriptionRule.class);
        rules = getRuntimeContext().getListState(stateDescriptor);
    }
}
