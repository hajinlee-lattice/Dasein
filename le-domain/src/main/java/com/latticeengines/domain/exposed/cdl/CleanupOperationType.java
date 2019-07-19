package com.latticeengines.domain.exposed.cdl;

public enum CleanupOperationType {
    ALL("All", false), //
    ALLDATA("AllData", false), //
    ALLDATAANDATTRCONFIG("AllDataAndAttrConfig", false), //
    BYDATERANGE("ByDateRange", false), //
    BYUPLOAD_ID("ByUpload_Id", true), //
    BYUPLOAD_ACPD("ByUpload_ACPD", true), //
    BYUPLOAD_MINDATE("ByUpload_MinDate", true), //
    BYUPLOAD_MINDATEANDACCOUNT("ByUpload_MinDateAndAccount", true), //
    ALLATTRCONFIG("AllAttrConfig", false), //
    ALLDATAANDMETADATA("AllDataAndMetaData", false);//

    private String operationType;
    private boolean needTransFlow;

    CleanupOperationType(String operationType, boolean needTransFlow) {
        this.operationType = operationType;
        this.needTransFlow = needTransFlow;
    }

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public boolean isNeedTransFlow() {
        return needTransFlow;
    }

    public void setNeedTransFlow(boolean needTransFlow) {
        this.needTransFlow = needTransFlow;
    }
}
