package com.latticeengines.domain.exposed.metadata;

public enum PredeterminedColumnDataTypes {
    Interest_esb__c("Interest_esb__c"), Interest_tcat__c("Interest_tcat__c"), kickboxAcceptAll(
            "kickboxAcceptAll"), Free_Email_Address__c("Free_Email_Address__c"), kickboxFree(
                    "kickboxFree"), Unsubscribed("Unsubscribed"), kickboxDisposable(
                            "kickboxDisposable"), HasAnypointLogin(
                                    "HasAnypointLogin"), HasCEDownload(
                                            "HasCEDownload"), HasEEDownload(
                                                    "HasEEDownload"), Lead_Source_Asset__c(
                                                            "Lead_Source_Asset__c"), kickboxStatus(
                                                                    "kickboxStatus"), SICCode(
                                                                            "SICCode"), Source_Detail__c(
                                                                                    "Source_Detail__c"), Cloud_Plan__c(
                                                                                            "Cloud_Plan__c");
    private String dataType;

    PredeterminedColumnDataTypes(String dataType) {
        this.dataType = dataType;
    }

    public static UserDefinedType returnUserDefinedType(String preColDateTypeStr) {
        if (preColDateTypeStr.startsWith("Activity_Count_")) {
            return UserDefinedType.NUMBER;
        }
        PredeterminedColumnDataTypes preColDateType = null;
        try {
            preColDateType = PredeterminedColumnDataTypes.valueOf(preColDateTypeStr);
        } catch (Exception e) {
            return null;
        }
        switch (preColDateType) {
            case Interest_esb__c:
            case Interest_tcat__c:
            case kickboxAcceptAll:
            case Free_Email_Address__c:
            case kickboxFree:
            case Unsubscribed:
            case kickboxDisposable:
            case HasAnypointLogin:
            case HasCEDownload:
            case HasEEDownload:
                return UserDefinedType.BOOLEAN;
            case Lead_Source_Asset__c:
            case kickboxStatus:
            case SICCode:
            case Source_Detail__c:
            case Cloud_Plan__c:
                return UserDefinedType.TEXT;
        }
        return null;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
}
