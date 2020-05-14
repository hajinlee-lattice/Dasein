package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class ImportFileInfo {

    @JsonProperty("feed_type")
    private String feedType;

    @JsonProperty("file_path")
    private String filePath;

    @JsonProperty("template_name")
    private String templateName;

    @JsonProperty("system_name")
    private String systemName;

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String getFeedType() {
        return feedType;
    }

    public void setFeedType(String feedType) {
        this.feedType = feedType;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getTemplateName() {
        return templateName;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }
}
