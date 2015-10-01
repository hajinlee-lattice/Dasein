package com.latticeengines.scoring.util;

import java.util.Map;

public class ModelAndLeadInfo {
    Map<String, ModelInfo> modelInfoMap;
    long totalleadNumber;

    public Map<String, ModelInfo> getModelInfoMap() {
        return this.modelInfoMap;
    }

    public long getTotalleadNumber() {
        return this.totalleadNumber;
    }

    public void setModelInfoMap(Map<String, ModelInfo> modelInfoMap) {
        this.modelInfoMap = modelInfoMap;
    }

    public void setTotalleadNumber(long totalleadNumber) {
        this.totalleadNumber = totalleadNumber;
    }

    static public class ModelInfo {
        private String modelId;
        private long leadNumber;

        public ModelInfo(String modelId, long leadNumber) {
            this.modelId = modelId;
            this.leadNumber = leadNumber;
        }

        public String getModelId() {
            return this.modelId;
        }

        public long getLeadNumber() {
            return this.leadNumber;
        }

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setLeadNumber(long leadNumber) {
            this.leadNumber = leadNumber;
        }
    }
}
