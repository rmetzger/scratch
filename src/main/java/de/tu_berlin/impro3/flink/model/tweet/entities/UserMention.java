package de.tu_berlin.impro3.flink.model.tweet.entities;

/**
 * Represents other Twitter users mentioned in the text of the
 * {@link de.tu_berlin.impro3.flink.model.tweet.Tweet}.
 */
public class UserMention {

    private long id;

    private String id_str;

    private String screen_name;

    private String name;

    private long[] indices;

    public UserMention() {
        this.id = 0L;
        this.id_str = "";
        this.screen_name = "";
        this.name = "";
        this.setIndices(new long[]{0L, 0L});
    }

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

    public String getScreen_name() {
        return screen_name;
    }

    public void setScreen_name(String screen_name) {
        this.screen_name = screen_name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long[] getIndices() {
        return indices;
    }

    public void setIndices(long[] indices) {
        this.indices = indices;
    }
}
