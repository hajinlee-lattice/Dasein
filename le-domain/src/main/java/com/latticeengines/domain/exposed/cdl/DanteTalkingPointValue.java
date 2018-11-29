package com.latticeengines.domain.exposed.cdl;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.TalkingPoint;

public class DanteTalkingPointValue {
    @JsonProperty(value = "BaseExternalID", index = 1)
    private String baseExternalID;

    @JsonProperty(value = "NotionName", index = 2)
    private String notionName = "DanteTalkingPoint";

    @JsonProperty(value = "Content", index = 3)
    private String content;

    @JsonProperty(value = "LastModified", index = 4)
    private Date lastModified;

    @JsonProperty(value = "Offset", index = 5)
    private int offset;

    @JsonProperty(value = "PlayID", index = 6)
    private String playID;

    @JsonProperty(value = "TalkingPointID", index = 7)
    private String talkingPointID;

    @JsonProperty(value = "Title", index = 8)
    private String title;

    // for jackson
    @SuppressWarnings("unused")
    private DanteTalkingPointValue() {
    }

    public DanteTalkingPointValue(TalkingPoint tp) {
        baseExternalID = tp.getName();
        content = tp.getContent();
        lastModified = tp.getUpdated();
        offset = tp.getOffset();
        playID = tp.getPlay().getName();
        talkingPointID = tp.getName();
        title = tp.getTitle();
    }

    public String getBaseExternalID() {
        return baseExternalID;
    }

    public void setBaseExternalID(String baseExternalID) {
        this.baseExternalID = baseExternalID;
    }

    public String getNotionName() {
        return notionName;
    }

    public void setNotionName(String notionName) {
        this.notionName = notionName;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public String getPlayID() {
        return playID;
    }

    public void setPlayID(String playID) {
        this.playID = playID;
    }

    public String getTalkingPointID() {
        return talkingPointID;
    }

    public void setTalkingPointID(String talkingPointID) {
        this.talkingPointID = talkingPointID;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
