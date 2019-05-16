package com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;

public class PrepareImportConfiguration extends BaseReportStepConfiguration {

    @JsonProperty("data_feed_task_id")
    private String dataFeedTaskId;

    @JsonProperty("source_bucket")
    private String sourceBucket;

    @JsonProperty("source_key")
    private String sourceKey;

    @JsonProperty("dest_bucket")
    private String destBucket;

    @JsonProperty("dest_key")
    private String destKey;

    @JsonProperty("backup_key")
    private String backupKey;

    @JsonProperty("email_info")
    private S3ImportEmailInfo emailInfo;

    public String getDataFeedTaskId() {
        return dataFeedTaskId;
    }

    public void setDataFeedTaskId(String dataFeedTaskId) {
        this.dataFeedTaskId = dataFeedTaskId;
    }

    public String getSourceBucket() {
        return sourceBucket;
    }

    public void setSourceBucket(String sourceBucket) {
        this.sourceBucket = sourceBucket;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public void setSourceKey(String sourceKey) {
        this.sourceKey = sourceKey;
    }

    public String getDestBucket() {
        return destBucket;
    }

    public void setDestBucket(String destBucket) {
        this.destBucket = destBucket;
    }

    public String getDestKey() {
        return destKey;
    }

    public void setDestKey(String destKey) {
        this.destKey = destKey;
    }

    public String getBackupKey() {
        return backupKey;
    }

    public void setBackupKey(String backupKey) {
        this.backupKey = backupKey;
    }

    public S3ImportEmailInfo getEmailInfo() {
        return emailInfo;
    }

    public void setEmailInfo(S3ImportEmailInfo emailInfo) {
        this.emailInfo = emailInfo;
    }

    @JsonIgnore
    public String getSourceFileName() {
        if (StringUtils.isEmpty(sourceKey) || sourceKey.lastIndexOf('/') < 0) {
            return sourceKey;
        }
        return sourceKey.substring(sourceKey.lastIndexOf('/') + 1);
    }
}
