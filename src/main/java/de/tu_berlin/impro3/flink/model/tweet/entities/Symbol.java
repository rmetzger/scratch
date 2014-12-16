package de.tu_berlin.impro3.flink.model.tweet.entities;

/**
 * An array of financial symbols starting with the dollar sign extracted from the
 * {@link de.tu_berlin.impro3.flink.model.tweet.Tweet} text.
 */

public class Symbol {

    private String text;

    private long[] indices;

    public Symbol() {
        this.text = "";
        this.setIndices(new long[]{0L, 0L});

    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public long[] getIndices() {
        return indices;
    }

    public void setIndices(long[] indices) {
        this.indices = indices;
    }
}
