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

    @JsonProperty("BatchSize")
    private Integer batchSize = 0;

    @JsonProperty("MinPid")
    private Long minPid;

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

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
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
