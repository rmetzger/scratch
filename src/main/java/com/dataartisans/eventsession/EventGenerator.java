package com.dataartisans.eventsession;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.XORShiftRandom;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates a defined number of concurrent active users
 */
public class EventGenerator implements ParallelSourceFunction<Event> {

    private final Random RND = new XORShiftRandom();

    // how many concurrently active users we generate
    private final long maxActiveUserCount;
    // how many actions each user performs before logging out again
    private final int actionsPerUser;
    // the number of keys
    private final long totalNumberOfUsers;
    private boolean running = true;
    private Map<Long, Integer> activeUsers = new HashMap<>();

    public EventGenerator(ParameterTool pt) {
        // TODO make parallelizable
        this.maxActiveUserCount = pt.getLong("maxActiveUserCount", 10_000);
        this.totalNumberOfUsers = pt.getLong("totalNumberOfUsers", 1_000_000);
        this.actionsPerUser = pt.getInt("actionsPerUser", 5);
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        long i = 0;
        while (running) {
            Event e = new Event();
            e.id = i++;
            if(activeUsers.size() < maxActiveUserCount) {
                // we need to create a new user
                long nextUserId = getNextUserId();
                if(activeUsers.put(nextUserId, 0) != null) {
                    throw new IllegalStateException("Fatal flaw in this code");
                }
                e.user = nextUserId;
                e.type = EventSessionWindowJob.EventType.LOGIN;
            } else {
                // there are enough users
                // TODO a hash map is probably not the best structure here :)
                Map.Entry<Long, Integer> someUser = (Map.Entry<Long, Integer>) activeUsers.entrySet().toArray()[RND.nextInt(activeUsers.size())];
                e.user = someUser.getKey();
                if(someUser.getValue() == actionsPerUser) {
                    // user has done enough --> log out
                    e.type = EventSessionWindowJob.EventType.LOGOUT;
                    activeUsers.remove(someUser.getKey());
                } else {
                    // perform regular auth action
                    e.type = EventSessionWindowJob.EventType.AUTH_ACTION;
                    activeUsers.put(someUser.getKey(), someUser.getValue() + 1);
                }
            }
            // use some fast RND here: (2.2 mio -> 2.6 mio)
            e.url = RandomStringUtils.random(10, 0, 0, true, true, null, RND);
            ctx.collect(e);
           // Thread.sleep(100);
        }
    }

    private long getNextUserId() {
        long next;
        do {
            next = ThreadLocalRandom.current().nextLong(this.totalNumberOfUsers);
        } while(activeUsers.containsKey(next));
        return next;
    }

    @Override
    public void cancel() {
        running = false;
    }
}
