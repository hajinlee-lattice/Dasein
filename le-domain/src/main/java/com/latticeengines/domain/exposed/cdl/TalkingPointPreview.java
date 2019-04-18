package com.latticeengines.domain.exposed.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TalkingPointPreview {
    private static final String defaultContextValue = "lpipreview";
    private static final String defaultNotionValue = "lead";

    @JsonProperty(index = 1)
    private String context = defaultContextValue;

    @JsonProperty(index = 2)
    private String notion = defaultNotionValue;

    @JsonProperty(index = 3)
    private DanteLeadNotionObject notionObject;

    public TalkingPointPreview() {
    }

    public TalkingPointPreview(List<DanteTalkingPointValue> talkingPoints) {
        notionObject = new DanteLeadNotionObject(talkingPoints);
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getNotion() {
        return notion;
    }

    public void setNotion(String notion) {
        this.notion = notion;
    }

    public DanteLeadNotionObject getNotionObject() {
        return notionObject;
    }

    public void setNotionObject(DanteLeadNotionObject notionObject) {
        this.notionObject = notionObject;
    }
}
