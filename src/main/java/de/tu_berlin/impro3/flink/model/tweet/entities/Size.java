package de.tu_berlin.impro3.flink.model.tweet.entities;

/**
 * An object showing available sizes for the media file.
 */
public class Size {

    private long w;

    private long h;

    private String resize;


    public Size(long width, long height, String resize) {

        this.w = width;
        this.h = height;
        this.resize = resize;

    }


    public long getWidth() {
        return w;
    }

    public void setWidth(long width) {
        this.w = width;
    }

    public long getHeight() {
        return h;
    }

    public void setHeight(long height) {
        this.h = height;
    }

    public String getResize() {
        return resize;
    }

    public void setResize(String resize) {
        this.resize = resize;
    }
}
