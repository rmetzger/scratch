package de.tu_berlin.impro3.flink.model.places;

public class Attributes {

    private String street_address;

    private String locality;

    private String region;

    private String iso3;

    private String postal_code;

    private String phone;

    private String twitter = "twitter";

    private String url;

    // in the API it is app:id !!
    private String appId;

    public Attributes() {

        this.setStreet_address("");
        this.setLocality("");
        this.setRegion("");
        this.setIso3("");
        this.setPostal_code("");
        this.setPhone("");
        this.setUrl("");
        this.setAppId("");
    }


    public String getStreet_address() {
        return street_address;
    }

    public void setStreet_address(String street_address) {
        this.street_address = street_address;
    }

    public String getLocality() {
        return locality;
    }

    public void setLocality(String locality) {
        this.locality = locality;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getIso3() {
        return iso3;
    }

    public void setIso3(String iso3) {
        this.iso3 = iso3;
    }

    public String getPostal_code() {
        return postal_code;
    }

    public void setPostal_code(String postal_code) {
        this.postal_code = postal_code;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getTwitter() {
        return twitter;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }
}
