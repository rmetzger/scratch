package com.dataartisans.eventsession;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * asdf
 */
public class EventSessionWindowAssigner extends MergingWindowAssigner<Event, EventSessionWindow> {

    @Override
    public Collection<EventSessionWindow> assignWindows(Event element, long timestamp, WindowAssignerContext context) {
        return Collections.singletonList(new EventSessionWindow(element));
    }

    @Override
    public Trigger<Event, EventSessionWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return new EventSessionWindowTrigger();
    }

    @Override
    public TypeSerializer<EventSessionWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new EventSessionWindowSerializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }

    @Override
    public void mergeWindows(Collection<EventSessionWindow> windows, MergeCallback<EventSessionWindow> callback) {
        if(windows.size() > 1) {
            // perform merge
            List<Event> events = new ArrayList<>();
            for(EventSessionWindow window: windows) {
                events.addAll(window.events);
            }
            EventSessionWindow mergedWindow = new EventSessionWindow();
            callback.merge(windows, mergedWindow);
        }
    }

    private class EventSessionWindowSerializer extends TypeSerializer<EventSessionWindow> {
        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<EventSessionWindow> duplicate() {
            return new EventSessionWindowSerializer();
        }

        @Override
        public EventSessionWindow createInstance() {
            return new EventSessionWindow();
        }

        @Override
        public EventSessionWindow copy(EventSessionWindow from) {
            return new EventSessionWindow(from);
        }

        @Override
        public EventSessionWindow copy(EventSessionWindow from, EventSessionWindow reuse) {
            reuse.events = from.events;
            return reuse;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(EventSessionWindow record, DataOutputView target) throws IOException {

        }

        @Override
        public EventSessionWindow deserialize(DataInputView source) throws IOException {
            return null;
        }

        @Override
        public EventSessionWindow deserialize(EventSessionWindow reuse, DataInputView source) throws IOException {
            return null;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {

        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }

        @Override
        public boolean canEqual(Object obj) {
            return false;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }
}
