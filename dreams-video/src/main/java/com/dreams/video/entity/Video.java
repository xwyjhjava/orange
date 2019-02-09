package com.dreams.video.entity;

import java.util.Date;


public class Video {

    private Integer type;
    private String title;
    private Date date;
    private String url;
    private String author;
    private Integer likenum;
    private Integer coinnum;
    private Integer collectionnum;
    private String brief;
    private String tag;
    private String comment;

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public Integer getLikenum() {
        return likenum;
    }

    public void setLikenum(Integer likenum) {
        this.likenum = likenum;
    }

    public Integer getCoinnum() {
        return coinnum;
    }

    public void setCoinnum(Integer coinnum) {
        this.coinnum = coinnum;
    }

    public Integer getCollectionnum() {
        return collectionnum;
    }

    public void setCollectionnum(Integer collectionnum) {
        this.collectionnum = collectionnum;
    }

    public String getBrief() {
        return brief;
    }

    public void setBrief(String brief) {
        this.brief = brief;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}
