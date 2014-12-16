package de.tu_berlin.impro3.flink.model.places;

import java.util.ArrayList;
import java.util.List;

/**
 * A series of longitude and latitude points, defining a box which will contain the Place entity
 * this bounding box is related to. Each point is an array in the form of [longitude, latitude].
 * Points are grouped into an array per bounding box. Bounding box arrays are wrapped in one
 * additional array to be compatible with the polygon notation.
 */
public class BoundingBox {

    private List<List<double[]>> coordinates = new ArrayList<List<double[]>>();

    private String type = "Polygon";

    public BoundingBox() {

    }

    public BoundingBox(List<double[]> points) {

        this.coordinates.add(points);

    }

    public List<List<double[]>> getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(List<List<double[]>> coordinates) {
        this.coordinates = coordinates;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
