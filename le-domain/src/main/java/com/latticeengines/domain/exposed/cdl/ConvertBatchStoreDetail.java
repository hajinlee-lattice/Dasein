package com.latticeengines.domain.exposed.cdl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ConvertBatchStoreDetail {

    @JsonProperty("task_unique_id")
    private String taskUniqueId;

    @JsonProperty("duplicate_map")
    private Map<String, String> duplicateMap;

    @JsonProperty("rename_map")
    private Map<String, String> renameMap;

    @JsonProperty("import_count")
    private Long importCount;

    @JsonProperty("data_tables")
    private List<String> dataTables;

    public String getTaskUniqueId() {
        return taskUniqueId;
    }

    public void setTaskUniqueId(String taskUniqueId) {
        this.taskUniqueId = taskUniqueId;
    }

    public Map<String, String> getDuplicateMap() {
        return duplicateMap;
    }

    public void setDuplicateMap(Map<String, String> duplicateMap) {
        this.duplicateMap = duplicateMap;
    }

    public Map<String, String> getRenameMap() {
        return renameMap;
    }

    public void setRenameMap(Map<String, String> renameMap) {
        this.renameMap = renameMap;
    }

    public Long getImportCount() {
        return importCount;
    }

    public void setImportCount(Long importCount) {
        this.importCount = importCount;
    }

    public List<String> getDataTables() {
        return dataTables;
    }

    public void setDataTables(List<String> dataTables) {
        this.dataTables = dataTables;
    }
}
