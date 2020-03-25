package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class UploadStats {

    @JsonProperty("import_application_pid")
    private Long importApplicationPid;

    @JsonProperty("import_success_cnt")
    private int importSuccessCnt;

    @JsonProperty("import_error_cnt")
    private int importErrorCnt;

    public Long getImportApplicationPid() {
        return importApplicationPid;
    }

    public void setImportApplicationPid(Long importApplicationPid) {
        this.importApplicationPid = importApplicationPid;
    }

    public int getImportSuccessCnt() {
        return importSuccessCnt;
    }

    public void setImportSuccessCnt(int importSuccessCnt) {
        this.importSuccessCnt = importSuccessCnt;
    }

    public int getImportErrorCnt() {
        return importErrorCnt;
    }

    public void setImportErrorCnt(int importErrorCnt) {
        this.importErrorCnt = importErrorCnt;
    }
}
