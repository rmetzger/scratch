package com.dataartisans;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Sessionization {

	private static final Logger LOG = LoggerFactory.getLogger(Sessionization.class);

	// ----------------------- user defined classes  -----------------------

	public enum EventType {
		LOGIN, // start of session
		LOGOUT, // end of session
		// some events:
		CHANGE_PASSWORD, ADD_POST, ADD_COMMENT, UPDATE_SETTINGS, SUPPORT_REQUEST
	}
	public static EventType getRandomEvent(Random rnd) {
		return EventType.values()[rnd.nextInt(EventType.values().length - 1)];
	}

	public static class Event {
		public long eventTimestamp;
		public EventType type;
		public int userIp;

		@Override
		public String toString() {
			return "Event{" +
					"eventTimestamp=" + eventTimestamp +
					", type=" + type +
					", userIp=" + userIp +
					'}';
		}
	}



	private static class SessionEventGenerator extends RichSourceFunction<Event> implements EventTimeSourceFunction<Event> {
		private boolean running = true;

		@Override
		public void run(SourceContext<Event> ctx) throws Exception {
			Random rnd = new Random();
			List<Integer> openSessions = new ArrayList<>();
			long realTime = 100; // start at time 100.
			while(running) {
				Thread.sleep(1000);
				// generate fuzzy time which can be at most +/- 100 off from the real time.
				long fuzzyTime = realTime + (long)(100 * (1 - rnd.nextGaussian()));
				if(openSessions.size() < 800) {
					int ip = rnd.nextInt();
					openSessions.add(ip);
					Event e = new Event();
					e.eventTimestamp = fuzzyTime;
					e.type = EventType.LOGIN;
					e.userIp = ip;
					ctx.collectWithTimestamp(e, e.eventTimestamp);
					LOG.info("Emitting event {}", e);
				}
				if(openSessions.size() > 10) {
					int session = rnd.nextInt(openSessions.size() - 1);
					int ip = openSessions.get(session);
					Event e = new Event();
					e.eventTimestamp = fuzzyTime;
					e.type = getRandomEvent(rnd);
					e.userIp = ip;
					ctx.collectWithTimestamp(e, e.eventTimestamp);
					LOG.info("Emitting event {}", e);
					if(e.type == EventType.LOGOUT) {
						openSessions.remove(session);
					}
				}

				// emit watermark every 10th element
				if(realTime % 10 == 0) {
					Watermark w = new Watermark(realTime - 100);
					ctx.emitWatermark(w);
					LOG.info("Emitting watermark {}", w);
				}
				// advance the clock
				realTime++;
			}
			ctx.close();
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Event> eventStream = see.addSource(new SessionEventGenerator());
		DataStream<Tuple2<Integer, Integer>> eventsPerUser = eventStream.keyBy("userIp").window(new SessionWindowAssigner())
			.trigger(new EventTimeWindowTrigger())
			.apply(new WindowFunction<Event, Tuple3<Integer, Integer, Long>, Tuple, TimeWindow>() {
				@Override
				public void apply(Tuple tuple, TimeWindow window, Iterable<Event> values, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
					int eventCount = 0;
					long start = 0, end = 0;
					for (Event e : values) {
						if(e.type == EventType.LOGIN) start = e.eventTimestamp;
						if(e.type == EventType.LOGOUT) end = e.eventTimestamp;
						eventCount++;
					}
					out.collect(new Tuple2<>((Integer) tuple.getField(0), eventCount, end - start));
				}
			});

		eventsPerUser.print();

		see.execute("Sessionization example");
	}

	private static class SessionWindowAssigner extends WindowAssigner<Event, TimeWindow> {
		@Override
		public Collection<TimeWindow> assignWindows(Event element, long timestamp) {
			return Collections.singletonList(new TimeWindow(timestamp, 100000L));
		}

		@Override
		public Trigger<Event, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
			// trigger window when logout event occured
			return null;
		}
	}
	private static class EventTimeWindowTrigger implements Trigger<Event, TimeWindow> {

		@Override
		public TriggerResult onElement(Event element, long timestamp, TimeWindow window, TriggerContext ctx) {
			if(element.type == EventType.LOGOUT) {
				return TriggerResult.FIRE_AND_PURGE;
			} else {
				return TriggerResult.CONTINUE;
			}
		}

		@Override
		public TriggerResult onTime(long time, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}

		@Override
		public Trigger<Event, TimeWindow> duplicate() {
			return new EventTimeWindowTrigger();
		}
	}

	private static class SessionWindow extends Window {

		@Override
		public long getStart() {
			return 0;
		}

		@Override
		public long getEnd() {
			return Long.MAX_VALUE;
		}

		@Override
		public long maxTimestamp() {
			return 0;
		}
	}
}
