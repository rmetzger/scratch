package de.tu_berlin.impro3.flink.model.tweet;

/**
 * Nullable. An collection of brief user objects (usually only one) indicating users who contributed
 * to the authorship of the {@link de.tu_berlin.impro3.flink.model.tweet.Tweet} on behalf of the
 * official tweet author.
 */
public class Contributors {


    private Long id;

    private String id_str;

    private String screenName;

    public Contributors() {

        this.id = 0L;
        this.id_str = "";
        this.screenName = "";

    }

    public Contributors(long id, String id_str, String screenName) {

        this.id = id;
        this.id_str = id_str;
        this.screenName = screenName;
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

    public void setId_str(String id_str) {
        this.id_str = id_str;
    }

    public String getScreenName() {
        return screenName;
    }

    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }


}
