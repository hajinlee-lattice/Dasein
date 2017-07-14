package com.latticeengines.dante.metadata;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

public class DanteLead {
    private final static String defaultContextValue = "lpipreview";
    private final static String defaultNotionValue = "lead";
    @JsonProperty(index = 1)
    private String context = defaultContextValue;

    @JsonProperty(index = 2)
    private String notion = defaultNotionValue;

    @JsonProperty(index = 3)
    private DanteLeadNotionObject notionObject;

    public DanteLead(List<DanteTalkingPoint> talkingPoints) {
        notionObject = new DanteLeadNotionObject(talkingPoints);
    }
}
