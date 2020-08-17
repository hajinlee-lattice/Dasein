package com.latticeengines.datacloud.match.domain;

import java.util.List;

public class TpsLookupResult {

    private List<String> recordIds;

    private ReturnCode returnCode = ReturnCode.Ok;

    public List<String> getRecordIds() {
        return recordIds;
    }

    public void setRecordIds(List<String> recordIds) {
        this.recordIds = recordIds;
    }

    public ReturnCode getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(ReturnCode returnCode) {
        this.returnCode = returnCode;
    }

    // each return code is a particular error (or ok) scenario
    public enum ReturnCode {
        Ok, //
        EmptyResult, // no record id for the given duns
        UnknownRemoteError, // unknown error when communicating with remote data store (DynamoDB)
        UnknownLocalError, // unknown exception within current jvm
    }

}
