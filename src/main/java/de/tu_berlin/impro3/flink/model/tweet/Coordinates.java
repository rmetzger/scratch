package de.tu_berlin.impro3.flink.model.tweet;

import com.google.gson.Gson;

/**
 * Nullable. Represents the geographic location of this
 * {@link de.tu_berlin.impro3.flink.model.tweet.Tweet} as reported by the user or client
 * application. The inner coordinates array is formatted as geoJSON longitude first, then latitude)
 */
public class Coordinates {

    private String type;

    private double[] coordinates = new double[2];

    public Coordinates() {

        type = "point";

    }

    public double[] getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(double[] coordinates) {
        this.coordinates = coordinates;
    }

    public void setCoordinates(double longitude, double latitude) {
        this.coordinates[0] = longitude;
        this.coordinates[1] = latitude;
    }

    public String getType() {
        return type;
    }


    public static void main(String[] args) {
        Gson gson = new Gson();
        Coordinates c = new Coordinates();
        System.out.println(gson.toJson(c));
    }
}
