package com.latticeengines.domain.exposed.datacloud.match;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class DunsGuideBookConfig extends TransformerConfig {
    // Lower value, higher book priority
    @JsonProperty("BookPriority")
    private Map<String, Integer> bookPriority;

    public Map<String, Integer> getBookPriority() {
        return bookPriority;
    }

    public void setBookPriority(Map<String, Integer> bookPriority) {
        this.bookPriority = bookPriority;
    }

}
