package com.dataartisans.eventsession;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.XORShiftRandom;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates a defined number of concurrent active users
 */
public class EventGenerator extends RichParallelSourceFunction<Event> {

    private final Random RND = new XORShiftRandom();

    // how many concurrently active users we generate
    private final int maxActiveUserCount;
    // how many actions each user performs before logging out again
    private final int actionsPerUser;
    // the number of keys
    private final long totalNumberOfUsers;
    private boolean running = true;
    private transient ActiveUsersStructure activeUsers;

    public EventGenerator(ParameterTool pt) {
        // TODO make parallelizable
        this.maxActiveUserCount = pt.getInt("maxActiveUserCount", 10);
        this.totalNumberOfUsers = pt.getLong("totalNumberOfUsers", 100);
        this.actionsPerUser = pt.getInt("actionsPerUser", 5);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        activeUsers = new ActiveUsersStructure(maxActiveUserCount);
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        long i = 0;
        while (running) {
            Event e = new Event();
            e.id = i;
            if(activeUsers.size() > 0 && RND.nextDouble() < 0.2) {
                e.type = EventSessionWindowJob.EventType.UNRELATED_ACTION;
                e.user = activeUsers.getRandom().f0;
            } else if (activeUsers.size() < maxActiveUserCount) {
                // we need to create a new user
                long nextUserId = getNextUserId();
                activeUsers.add(nextUserId, 0);
                e.user = nextUserId;
                e.type = EventSessionWindowJob.EventType.LOGIN;
            } else {
                // there are enough users
                Tuple2<Long, Integer> someUser = activeUsers.getRandom();
                e.user = someUser.f0;
                if (someUser.f1 == actionsPerUser) {
                    // user has done enough --> log out
                    e.type = EventSessionWindowJob.EventType.LOGOUT;
                    activeUsers.remove(someUser.f0);
                } else {
                    // perform regular auth action
                    e.type = EventSessionWindowJob.EventType.AUTH_ACTION;
                    someUser.f1++;
                }
            }
            // use some fast RND here: (2.2 mio -> 2.6 mio)
            e.url = RandomStringUtils.random(10, 0, 0, true, true, null, RND);
            ctx.collectWithTimestamp(e, i);
            i++;
         //   Thread.sleep(100);
        }
    }

    private long getNextUserId() {
        long next;
        do {
            next = ThreadLocalRandom.current().nextLong(this.totalNumberOfUsers);
        } while(activeUsers.contains(next));
        return next;
    }

    @Override
    public void cancel() {
        running = false;
    }


    /**
     * Compute efficient structure for maintaining all active users.
     *
     * Elements are unique.
     */
    private class ActiveUsersStructure {
        private Set<Long> activeSet;
        private Tuple2<Long, Integer>[] active;
        private int nextFreeHint = 0;

        public ActiveUsersStructure(int maxActiveUserCount) {
            this.active = (Tuple2<Long, Integer>[]) new Tuple2[maxActiveUserCount];
            this.activeSet = new HashSet<>(maxActiveUserCount);
        }

        public boolean contains(long key) {
            return activeSet.contains(key);
        }

        public int size() {
            return activeSet.size();
        }

        public void add(long key, int value) {
            // fast path for filling last remove spot
            if(nextFreeHint >= 0 && active[nextFreeHint] == null) {
                active[nextFreeHint] = new Tuple2<>(key, value);
                nextFreeHint = -1;
                // add to set as well.
                if(!activeSet.add(key)) {
                    throw new IllegalStateException("Fatal flaw in this code");
                }
                return;
            }

            // search through array to find empty spot
            for(int i = 0; i < active.length; i++) {
                if(active[i] == null) {
                    active[i] = new Tuple2<>(key, value);
                    // add to set as well.
                    if(!activeSet.add(key)) {
                        throw new IllegalStateException("Fatal flaw in this code");
                    }
                    return;
                }
            }
            throw new IllegalStateException("Array is full");
        }

        public void remove(long key) {
            if(!activeSet.remove(key)) {
                throw new IllegalStateException("Key not known in set " + key);
            }
            for(int i = 0; i < active.length; i++) {
                if(active[i].f0 == key) {
                    active[i] = null;
                    nextFreeHint = i;
                    return;
                }
            }
            throw new IllegalStateException("Key not known in array " + key);
        }

        public Tuple2<Long,Integer> getRandom() {
            Tuple2<Long,Integer> candid;
            do {
                candid = active[RND.nextInt(active.length)];
            } while(candid == null);
            return candid;
        }
    }
}
