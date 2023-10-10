package co.decodable.flink.example.iot.operators;

import org.apache.flink.api.common.functions.RichMapFunction;

import co.decodable.flink.example.iot.types.DeviceEvent;

public class MapDeviceEventsFromJSON extends RichMapFunction<String, DeviceEvent> {

    @Override
    public DeviceEvent map(String value) throws Exception {
        // TODO parse Json 'value' to DeviceEvent POJO
        return null;
    }
}
