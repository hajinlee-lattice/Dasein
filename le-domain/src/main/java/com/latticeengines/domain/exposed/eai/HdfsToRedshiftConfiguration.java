package com.latticeengines.domain.exposed.eai;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;

public class HdfsToRedshiftConfiguration extends ExportConfiguration {

    @JsonProperty("redshift_table_config")
    @NotNull
    private RedshiftTableConfiguration redshiftTableConfiguration;

    @JsonProperty("create_new")
    private boolean createNew = false;

    @JsonProperty("append")
    private boolean append = false;

    @JsonProperty("skip_s3_upload")
    private boolean skipS3Upload = false;

    @JsonProperty("cleanup_s3")
    private boolean cleanupS3 = false;

    @JsonProperty("no_split")
    private boolean noSplit = false;

    public RedshiftTableConfiguration getRedshiftTableConfiguration() {
        return redshiftTableConfiguration;
    }

    public void setRedshiftTableConfiguration(RedshiftTableConfiguration redshiftTableConfiguration) {
        this.redshiftTableConfiguration = redshiftTableConfiguration;
    }

    public boolean isCreateNew() {
        return createNew;
    }

    public void setCreateNew(boolean createNew) {
        this.createNew = createNew;
    }

    public boolean isAppend() {
        return append;
    }

    public void setAppend(boolean append) {
        this.append = append;
    }

    public boolean isSkipS3Upload() {
        return skipS3Upload;
    }

    public void setSkipS3Upload(boolean skipS3Upload) {
        this.skipS3Upload = skipS3Upload;
    }

    public boolean isCleanupS3() {
        return cleanupS3;
    }

    public void setCleanupS3(boolean cleanupS3) {
        this.cleanupS3 = cleanupS3;
    }

    public boolean isNoSplit() {
        return noSplit;
    }

    public void setNoSplit(boolean noSplit) {
        this.noSplit = noSplit;
    }
}
