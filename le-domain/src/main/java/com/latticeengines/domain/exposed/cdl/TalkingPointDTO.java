package com.latticeengines.domain.exposed.cdl;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.util.TalkingPointUtils;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.query.AttributeLookup;

public class TalkingPointDTO {

    @JsonProperty("pid")
    private Long pid;

    @JsonProperty("name")
    private String name;

    @JsonProperty("playname")
    private String playName;

    @JsonProperty("playDisplayName")
    private String playDisplayName;

    @JsonProperty("title")
    private String title;

    @JsonProperty("content")
    private String content;

    @JsonProperty("attributes")
    private Set<AttributeLookup> attributes = new HashSet<>();

    @JsonProperty("offset")
    private int offset;

    @JsonProperty("created")
    private Date created;

    @JsonProperty("updated")
    private Date updated;

    public TalkingPointDTO() {
    }

    public TalkingPointDTO(TalkingPoint tp) {
        pid = tp.getPid();
        name = tp.getName();
        playName = tp.getPlay().getName();
        playDisplayName = tp.getPlay().getDisplayName();
        title = tp.getTitle();
        content = tp.getContent();
        if (StringUtils.isNotEmpty(content)) {
            attributes = TalkingPointUtils.extractAttributes(content);
        }
        offset = tp.getOffset();
        created = tp.getCreated();
        updated = tp.getUpdated();
    }

    public TalkingPointDTO(PublishedTalkingPoint tp) {
        pid = tp.getPid();
        name = tp.getName();
        playName = tp.getPlayName();
        title = tp.getTitle();
        content = tp.getContent();
        if (StringUtils.isNotEmpty(content)) {
            attributes = TalkingPointUtils.extractAttributes(content);
        }
        offset = tp.getOffset();
        created = tp.getCreated();
        updated = tp.getUpdated();
    }

    public TalkingPointDTO(PublishedTalkingPoint tp, String playDisplayName) {
        this(tp);
        this.playDisplayName = playDisplayName;
    }

    public TalkingPoint convertToTalkingPoint(Play play) {
        TalkingPoint tp = new TalkingPoint();
        tp.setPid(getPid());
        tp.setName(getName());
        tp.setContent(getContent());
        tp.setCreated(getCreated());
        tp.setOffset(getOffset());
        tp.setPlay(play);
        tp.setTitle(getTitle());
        tp.setUpdated(getUpdated());
        return tp;
    }

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPlayName() {
        return playName;
    }

    public void setPlayName(String playName) {
        this.playName = playName;
    }

    public String getPlayDisplayName() {
        return playDisplayName;
    }

    public void setPlayDisplayName(String playDisplayName) {
        this.playDisplayName = playDisplayName;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Set<AttributeLookup> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<AttributeLookup> attributes) {
        this.attributes = attributes;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Date getUpdated() {
        return updated;
    }

    public void setUpdated(Date updated) {
        this.updated = updated;
    }
}
