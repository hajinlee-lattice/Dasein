package com.latticeengines.domain.exposed.pls;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class S3ImportTemplateDisplay {
    @JsonProperty("Type")
    private String type;
    @JsonProperty("Path")
    private String path;
    @JsonProperty("TemplateName")
    private String templateName;
    @JsonProperty("LastImported")
    private Date lastImported;
    @JsonProperty("Status")
    private JobStatus status;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getTemplateName() {
        return templateName;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
    }

    public Date getLastImported() {
        return lastImported;
    }

    public void setLastImported(Date lastImported) {
        this.lastImported = lastImported;
    }

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
