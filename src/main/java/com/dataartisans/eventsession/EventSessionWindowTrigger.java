package com.dataartisans.eventsession;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;

/**
 * Created by robert on 11/26/16.
 */
public class EventSessionWindowTrigger extends Trigger<Event, EventSessionWindow> {
    @Override
    public TriggerResult onElement(Event element, long timestamp, EventSessionWindow window, TriggerContext ctx) throws Exception {
        // fire window if we see LOGOUT event
        if (element.type == EventSessionWindowJob.EventType.LOGOUT) {
            return TriggerResult.FIRE_AND_PURGE;
        }

        // keep collecting events
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, EventSessionWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, EventSessionWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(EventSessionWindow window, TriggerContext ctx) throws Exception {

    }
}
