package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ImportActionConfiguration extends ActionConfiguration {

    @JsonProperty("workflow_id")
    private Long workflowId;
    @JsonProperty("data_feed_task_id")
    private String dataFeedTaskId;
    @JsonProperty("original_filename")
    private String originalFilename;
    @JsonProperty("import_count")
    private Long importCount;
    @JsonProperty("registered_tables")
    private List<String> registeredTables;
    @JsonProperty("mock_completed")
    private Boolean mockCompleted;

    public ImportActionConfiguration() {

    }

    public Long getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(Long workflowId) {
        this.workflowId = workflowId;
    }

    public String getDataFeedTaskId() {
        return dataFeedTaskId;
    }

    public void setDataFeedTaskId(String dataFeedTaskId) {
        this.dataFeedTaskId = dataFeedTaskId;
    }

    public String getOriginalFilename() {
        return originalFilename;
    }

    public void setOriginalFilename(String originalFilename) {
        this.originalFilename = originalFilename;
    }

    public Long getImportCount() {
        if (importCount == null) {
            return 0L;
        } else {
            return importCount;
        }
    }

    public void setImportCount(Long importCount) {
        this.importCount = importCount;
    }

    public List<String> getRegisteredTables() {
        return registeredTables;
    }

    public void setRegisteredTables(List<String> registeredTables) {
        this.registeredTables = registeredTables;
    }

    public Boolean getMockCompleted() {
        return mockCompleted;
    }

    public void setMockCompleted(Boolean mockCompleted) {
        this.mockCompleted = mockCompleted;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    public String serialize() {
        return toString();
    }
}
