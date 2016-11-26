package com.dataartisans.eventsession;

/**
 * Created by robert on 11/26/16.
 */
public class Event {
    public EventSessionWindowJob.EventType type;
    public long id;
    public String url;
    public long user;

    @Override
    public String toString() {
        return "Event{" +
                "type=" + type +
                ", id=" + id +
                ", url='" + url + '\'' +
                ", user=" + user +
                '}';
    }
}
