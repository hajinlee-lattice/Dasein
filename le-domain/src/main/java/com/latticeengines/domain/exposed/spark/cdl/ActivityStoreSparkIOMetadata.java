package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;
import java.util.Map;

public class ActivityStoreSparkIOMetadata {

    private Map<String, Details> metadata; // id (streamId, groupId, etc.) -> details

    public Map<String, Details> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Details> metadata) {
        this.metadata = metadata;
    }

    public static class Details {

        private int startIdx; // start index of input df sequence for id (streamId, groupId, etc.)

        private List<String> labels; // labels for dataFrames in a section of input (periodName, etc.)

        public int getStartIdx() {
            return startIdx;
        }

        public void setStartIdx(int startIdx) {
            this.startIdx = startIdx;
        }

        public List<String> getLabels() {
            return labels;
        }

        public void setLabels(List<String> labels) {
            this.labels = labels;
        }
    }
}
