package de.tu_berlin.impro3.flink.model.tweet.entities;

import java.util.ArrayList;
import java.util.List;

/**
 * Entities which have been parsed out of the text of the
 * {@link de.tu_berlin.impro3.flink.model.tweet.Tweet}.
 */
public class Entities {

    private List<HashTags> hashtags;

    private List<Media> media;

    private List<URL> urls;

    private List<UserMention> user_mentions;

    private List<Symbol> symbols;

    public Entities() {

        hashtags = new ArrayList<HashTags>();
        media = new ArrayList<Media>();
        urls = new ArrayList<URL>();
        user_mentions = new ArrayList<UserMention>();
        symbols = new ArrayList<Symbol>();

    }

    public List<HashTags> getHashtags() {
        return hashtags;
    }

    public void setHashtags(List<HashTags> hashtags) {
        this.hashtags = hashtags;
    }

    public List<Media> getMedia() {
        return media;
    }

    public void setMedia(List<Media> media) {
        this.media = media;
    }

    public List<URL> getUrls() {
        return urls;
    }

    public void setUrls(List<URL> urls) {
        this.urls = urls;
    }

    public List<UserMention> getUser_mentions() {
        return user_mentions;
    }

    public void setUser_mentions(List<UserMention> user_mentions) {
        this.user_mentions = user_mentions;
    }


    public List<Symbol> getSymbols() {
        return symbols;
    }

    public void setSymbols(List<Symbol> symbols) {
        this.symbols = symbols;
    }

}
