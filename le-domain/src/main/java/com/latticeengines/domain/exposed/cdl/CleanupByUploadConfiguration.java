package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CleanupByUploadConfiguration extends CleanupOperationConfiguration {

    @JsonProperty("file_path")
    private String filePath;

}
