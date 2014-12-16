package de.tu_berlin.impro3.flink.model.tweet.entities;

/**
 * Represents URLs included in the text of a Tweet or within textual fields of a
 * {@link de.tu_berlin.impro3.flink.model.User.Users} object.
 */
public class URL {

    private String url;

    private String display_url;

    private String expanded_url;

    private long[] indices;

    public URL() {
        this.url = "";
        this.display_url = "";
        this.expanded_url = "";
        this.setIndices(new long[]{0L, 0L});
    }


    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDisplay_url() {
        return display_url;
    }

    public void setDisplay_url(String display_url) {
        this.display_url = display_url;
    }

    public String getExpanded_url() {
        return expanded_url;
    }

    public void setExpanded_url(String expanded_url) {
        this.expanded_url = expanded_url;
    }

    public long[] getIndices() {
        return indices;
    }

    public void setIndices(long[] indices) {
        this.indices = indices;
    }
}
