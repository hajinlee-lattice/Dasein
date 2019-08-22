package com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ConvertBatchStoreToImportServiceConfiguration extends BaseConvertBatchStoreServiceConfiguration {

    @JsonProperty("convert_info_pid")
    private Long convertInfoPid;

    public Long getConvertInfoPid() {
        return convertInfoPid;
    }

    public void setConvertInfoPid(Long convertInfoPid) {
        this.convertInfoPid = convertInfoPid;
    }
}
