package com.latticeengines.scoring.util;

import java.util.Map;

public class ModelAndRecordInfo {
    Map<String, ModelInfo> modelInfoMap;
    long totalRecordCount;

    public Map<String, ModelInfo> getModelInfoMap() {
        return this.modelInfoMap;
    }

    public long getTotalRecordCount() {
        return this.totalRecordCount;
    }

    public void setModelInfoMap(Map<String, ModelInfo> modelInfoMap) {
        this.modelInfoMap = modelInfoMap;
    }

    public void setTotalRecordCountr(long totalRecordCount) {
        this.totalRecordCount = totalRecordCount;
    }

    static public class ModelInfo {
        private String modelGuid;
        private long recordCount;

        public ModelInfo(String modelGuid, long recordCount) {
            this.modelGuid = modelGuid;
            this.recordCount = recordCount;
        }

        public String getModelGuid() {
            return this.modelGuid;
        }

        public long getRecordCount() {
            return this.recordCount;
        }

        public void setModelGuid(String modelGuid) {
            this.modelGuid = modelGuid;
        }

        public void setRecordCount(long recordCount) {
            this.recordCount = recordCount;
        }
    }
}
