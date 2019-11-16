package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchMode;

public class PatchBookConfiguration extends ProviderConfiguration {

    @JsonProperty("BookType")
    private PatchBook.Type bookType;

    @JsonProperty("PatchMode")
    private PatchMode patchMode;

    @JsonProperty("SkipValidation")
    private boolean skipValidation;

    // Total number of batches in ingestion job.
    // Only for testing purpose, don't recommend to have a fixed value for
    // production
    // job
    @JsonProperty("BatchCnt")
    private Integer batchCnt;

    // Min PID in PatchBook table
    // Only for testing purpose, don't recommend to have a fixed value for
    // production job
    // Only when both minPid and maxPid are provided, they will be honored,
    // otherwise get minPid and maxPid from PatchBook table
    @JsonProperty("MinPid")
    private Long minPid;

    // Max PID in PatchBook table
    // Only for testing purpose, don't recommend to have a fixed value for
    // production job
    // Only when both minPid and maxPid are provided, they will be honored,
    // otherwise get minPid and maxPid from PatchBook table
    @JsonProperty("MaxPid")
    private Long maxPid;

    // Don't allow multiple PatchBook ingestion running at same time
    @Override
    @JsonProperty("ConcurrentNum")
    public Integer getConcurrentNum() {
        return 1;
    }

    @Override
    @JsonProperty("ConcurrentNum")
    public void setConcurrentNum(Integer concurrentNum) {
        this.concurrentNum = 1;
    }

    public Integer getBatchCnt() {
        return batchCnt;
    }

    public void setBatchCnt(Integer batchCnt) {
        this.batchCnt = batchCnt;
    }

    public PatchBook.Type getBookType() {
        return bookType;
    }

    public void setBookType(PatchBook.Type bookType) {
        this.bookType = bookType;
    }

    public PatchMode getPatchMode() {
        return patchMode;
    }

    public void setPatchMode(PatchMode patchMode) {
        this.patchMode = patchMode;
    }

    public boolean isSkipValidation() {
        return skipValidation;
    }

    public void setSkipValidation(boolean skipValidation) {
        this.skipValidation = skipValidation;
    }

    public Long getMinPid() {
        return minPid;
    }

    public void setMinPid(Long minPid) {
        this.minPid = minPid;
    }

    public Long getMaxPid() {
        return maxPid;
    }

    public void setMaxPid(Long maxPid) {
        this.maxPid = maxPid;
    }
}
