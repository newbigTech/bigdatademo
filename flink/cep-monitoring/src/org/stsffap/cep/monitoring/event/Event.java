package org.stsffap.cep.monitoring.event;

public class Event {
    private String campaign_id;

    private String ad_id;

    private Float timestamp;

    private String country;

    private String os;

    private String dev_id;

    private String model;

    private int age;

    private String gender;

    private String pub_id;

    private String type;

    public Event(String campaign_id, String ad_id, Float timestamp, String country, String os, String dev_id, String model, int age, String gender, String pub_id, String type) {
        this.campaign_id = campaign_id;
        this.ad_id = ad_id;
        this.timestamp = timestamp;
        this.country = country;
        this.os = os;
        this.dev_id = dev_id;
        this.model = model;
        this.age = age;
        this.gender = gender;
        this.pub_id = pub_id;
        this.type = type;
    }


    public String toString(){
        return new String("Ad_id: " + getAd_id());
    }

    public String getCampaign_id() {
        return campaign_id;
    }

    public String getAd_id() {
        return ad_id;
    }

    public Float getTimestamp() {
        return timestamp;
    }

    public String getCountry() {
        return country;
    }

    public String getOs() {
        return os;
    }

    public String getDev_id() {
        return dev_id;
    }

    public String getModel() {
        return model;
    }

    public int getAge() {
        return age;
    }

    public String getGender() {
        return gender;
    }

    public String getPub_id() {
        return pub_id;
    }

    public String getType() {
        return type;
    }

}
