package com.latticeengines.domain.exposed.datacloud.statistics;

import java.util.List;
import java.util.Map;

public class TopNAttributes {
    private Map<String, List<TopAttribute>> topAttributes;

    public Map<String, List<TopAttribute>> getTopAttributes() {
        return topAttributes;
    }

    public void setTopAttributes(Map<String, List<TopAttribute>> topAttributes) {
        this.topAttributes = topAttributes;
    }

    public static class TopAttribute {
        private String attribute;

        private Integer nonNullCount;

        public String getAttribute() {
            return attribute;
        }

        public void setAttribute(String attribute) {
            this.attribute = attribute;
        }

        public Integer getNonNullCount() {
            return nonNullCount;
        }

        public void setNonNullCount(Integer nonNullCount) {
            this.nonNullCount = nonNullCount;
        }
    }
}
