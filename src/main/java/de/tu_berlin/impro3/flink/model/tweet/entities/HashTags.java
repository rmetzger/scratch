package de.tu_berlin.impro3.flink.model.tweet.entities;

/**
 * Represents hashtags which have been parsed out of the
 * {@link de.tu_berlin.impro3.flink.model.tweet.Tweet} text.
 */

public class HashTags {

    private long[] indices = new long[2];

    private String text;


    public long[] getIndices() {
        return indices;
    }

    public void setIndices(long[] indices) {
        this.indices = indices;
    }

    public void setIndices(long start, long end) {
        this.indices[0] = start;
        this.indices[1] = end;

    }

    public String getText() {
        return text;
    }

    public void setText(String text, boolean hashExist) {
        if (hashExist)
            this.text = text.substring((int) indices[0] + 1);
        else
            this.text = text;
    }

}
