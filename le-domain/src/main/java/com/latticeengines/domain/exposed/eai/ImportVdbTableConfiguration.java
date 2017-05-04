package com.latticeengines.domain.exposed.eai;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;

import java.util.List;

public class ImportVdbTableConfiguration {

    @JsonProperty("collection_identifier")
    private String collectionIdentifier;

    @JsonProperty("data_category")
    private String dataCategory;

    @JsonProperty("total_rows")
    private int totalRows;

    @JsonProperty("batch_size")
    private int batchSize;

    @JsonProperty("vdb_query_handle")
    private String vdbQueryHandle;

    @JsonProperty("merge_rule")
    private String mergeRule;

    @JsonProperty("create_table_rule")
    private String createTableRule;

    @JsonProperty("extract_path")
    private String extractPath;

    @JsonProperty("vdb_spec_metadata")
    private List<VdbSpecMetadata> metadataList;

    public String getCollectionIdentifier() {
        return collectionIdentifier;
    }

    public void setCollectionIdentifier(String collectionIdentifier) {
        this.collectionIdentifier = collectionIdentifier;
    }

    public String getDataCategory() {
        return dataCategory;
    }

    public void setDataCategory(String dataCategory) {
        this.dataCategory = dataCategory;
    }

    public int getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(int totalRows) {
        this.totalRows = totalRows;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getVdbQueryHandle() {
        return vdbQueryHandle;
    }

    public void setVdbQueryHandle(String vdbQueryHandle) {
        this.vdbQueryHandle = vdbQueryHandle;
    }

    public List<VdbSpecMetadata> getMetadataList() {
        return metadataList;
    }

    public void setMetadataList(List<VdbSpecMetadata> metadataList) {
        this.metadataList = metadataList;
    }

    public String getMergeRule() {
        return mergeRule;
    }

    public void setMergeRule(String mergeRule) {
        this.mergeRule = mergeRule;
    }

    public String getCreateTableRule() {
        return createTableRule;
    }

    public void setCreateTableRule(String createTableRule) {
        this.createTableRule = createTableRule;
    }

    public String getExtractPath() {
        return extractPath;
    }

    public void setExtractPath(String extractPath) {
        this.extractPath = extractPath;
    }

}
