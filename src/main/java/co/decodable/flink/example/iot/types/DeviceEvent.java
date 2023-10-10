package co.decodable.flink.example.iot.types;

/** Represents an event from an IoT device. */
public class DeviceEvent {

    private String deviceId;
    private String customerId;
    private Long time;
    private EventType type;

    public DeviceEvent() {}

    public DeviceEvent(String deviceId, Long time, EventType type) {
        this.deviceId = deviceId;
        this.time = time;
        this.type = type;
    }

    public String getCustomerId() {
        return customerId;
    }
}
