package de.tu_berlin.impro3.flink.model.tweet;


import de.tu_berlin.impro3.flink.model.User.Users;
import de.tu_berlin.impro3.flink.model.places.Places;
import de.tu_berlin.impro3.flink.model.tweet.entities.Entities;

import java.util.ArrayList;
import java.util.List;

public class Tweet {

    public List<Contributors> contributors;

    private Coordinates coordinates;

    private String created_at;

    private Entities entities;

    private long favorite_count;

    private boolean favorited;

    private String filter_level;

    private long id;

    private String id_str;

    private String in_reply_to_screen_name;

    private long in_reply_to_status_id;

    private String in_reply_to_status_id_str;

    private long in_reply_to_user_id;

    private String in_reply_to_user_id_str;

    private String lang;

    // Places
    private Places place;

    private boolean possibly_sensitive;

    private long retweet_count;

    private boolean retweeted;

    private String source;

    private String text;

    private boolean truncated;

    private Users user;


    public Tweet() {

        contributors = new ArrayList<Contributors>();
        coordinates = new Coordinates();
        entities = new Entities();
        place = new Places();
        user = new Users();
    }

    public List<Contributors> getContributors() {
        return contributors;
    }

    public void setContributors(List<Contributors> contributors) {
        this.contributors = contributors;
    }

    public Coordinates getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(Coordinates coordinates) {
        this.coordinates = coordinates;
    }

    public String getCreated_at() {
        return created_at;
    }

    public void setCreated_at(String created_at) {
        this.created_at = created_at;
    }

    public Entities getEntities() {
        return entities;
    }

    public void setEntities(Entities entities) {
        this.entities = entities;
    }

    public long getFavorite_count() {
        return favorite_count;
    }

    public void setFavorite_count(long favorite_count) {
        this.favorite_count = favorite_count;
    }

    public boolean isFavorited() {
        return favorited;
    }

    public void setFavorited(boolean favorited) {
        this.favorited = favorited;
    }

    public String getFilter_level() {
        return filter_level;
    }

    public void setFilter_level(String filter_level) {
        this.filter_level = filter_level;
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

    public String getIn_reply_to_screen_name() {
        return in_reply_to_screen_name;
    }

    public void setIn_reply_to_screen_name(String in_reply_to_screen_name) {
        this.in_reply_to_screen_name = in_reply_to_screen_name;
    }


    public long getIn_reply_to_status_id() {
        return in_reply_to_status_id;
    }

    public void setIn_reply_to_status_id(long in_reply_to_status_id) {
        this.in_reply_to_status_id = in_reply_to_status_id;
    }

    public String getIn_reply_to_status_id_str() {
        return in_reply_to_status_id_str;
    }

    public void setIn_reply_to_status_id_str(String in_reply_to_status_id_str) {
        this.in_reply_to_status_id_str = in_reply_to_status_id_str;
    }

    public long getIn_reply_to_user_id() {
        return in_reply_to_user_id;
    }

    public void setIn_reply_to_user_id(long in_reply_to_user_id) {
        this.in_reply_to_user_id = in_reply_to_user_id;
    }

    public String getIn_reply_to_user_id_str() {
        return in_reply_to_user_id_str;
    }

    public void setIn_reply_to_user_id_str(String in_reply_to_user_id_str) {
        this.in_reply_to_user_id_str = in_reply_to_user_id_str;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public Places getPlace() {
        return place;
    }

    public void setPlace(Places place) {
        this.place = place;
    }

    public boolean getPossibly_sensitive() {
        return possibly_sensitive;
    }

    public void setPossibly_sensitive(boolean possibly_sensitive) {
        this.possibly_sensitive = possibly_sensitive;
    }

    public long getRetweet_count() {
        return retweet_count;
    }

    public void setRetweet_count(long retweet_count) {
        this.retweet_count = retweet_count;
    }

    public boolean isRetweeted() {
        return retweeted;
    }

    public void setRetweeted(boolean retweeted) {
        this.retweeted = retweeted;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public boolean isTruncated() {
        return truncated;
    }

    public void setTruncated(boolean truncated) {
        this.truncated = truncated;
    }

    public Users getUser() {
        return user;
    }

    public void setUser(Users user) {
        this.user = user;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "contributors=" + contributors +
                ", coordinates=" + coordinates +
                ", created_at='" + created_at + '\'' +
                ", entities=" + entities +
                ", favorite_count=" + favorite_count +
                ", favorited=" + favorited +
                ", filter_level='" + filter_level + '\'' +
                ", id=" + id +
                ", id_str='" + id_str + '\'' +
                ", in_reply_to_screen_name='" + in_reply_to_screen_name + '\'' +
                ", in_reply_to_status_id=" + in_reply_to_status_id +
                ", in_reply_to_status_id_str='" + in_reply_to_status_id_str + '\'' +
                ", in_reply_to_user_id=" + in_reply_to_user_id +
                ", in_reply_to_user_id_str='" + in_reply_to_user_id_str + '\'' +
                ", lang='" + lang + '\'' +
                ", place=" + place +
                ", possibly_sensitive=" + possibly_sensitive +
                ", retweet_count=" + retweet_count +
                ", retweeted=" + retweeted +
                ", source='" + source + '\'' +
                ", text='" + text + '\'' +
                ", truncated=" + truncated +
                ", user=" + user +
                '}';
    }
}
