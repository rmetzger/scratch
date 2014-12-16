package de.tu_berlin.impro3.flink.model.tweet;

/**
 * Details the {@link de.tu_berlin.impro3.flink.model.tweet.Tweet} ID of the user’s own retweet (if
 * existent) of this {@link de.tu_berlin.impro3.flink.model.tweet.Tweet}.
 */
public class CurrentUserRetweet {

    private long id;

    private String id_str;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getId_str() {
        return id_str;
    }

    public void setId_str() {
        this.id_str = Long.toString(id);
    }
}
