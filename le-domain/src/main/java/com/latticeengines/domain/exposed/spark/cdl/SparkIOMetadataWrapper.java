package com.latticeengines.domain.exposed.spark.cdl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class SparkIOMetadataWrapper {

    private Map<String, Partition> metadata = new HashMap<>(); // id (streamId, groupId, etc.) -> partition

    public Map<String, Partition> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Partition> metadata) {
        this.metadata = metadata;
    }

    public void addPartition(@NotNull String partitionId, @NotNull Partition partition) {
        metadata.put(partitionId, partition);
    }

    public static class Partition {

        private int startIdx; // start index of input df sequence for id (streamId, groupId, etc.)

        private List<String> labels; // labels for dataFrames in a section of input (periodName, etc.)

        public Partition() {
        }

        public Partition(int startIdx, List<String> labels) {
            this.startIdx = startIdx;
            this.labels = labels;
        }

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
