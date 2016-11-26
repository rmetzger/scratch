package com.dataartisans.eventsession;

import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by robert on 11/26/16.
 */
public class EventSessionWindow extends Window {
    List<Event> events;

    public EventSessionWindow() {

    }

    public EventSessionWindow(EventSessionWindow from) {
        this.events = new ArrayList<>(from.events);
    }

    public EventSessionWindow(Event initialElement) {
        this.events = new ArrayList<>();
        events.add(initialElement);
    }

    @Override
    public long maxTimestamp() {
        return Long.MAX_VALUE;
    }
}
