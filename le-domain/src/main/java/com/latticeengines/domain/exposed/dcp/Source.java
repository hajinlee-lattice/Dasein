package com.latticeengines.domain.exposed.dcp;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class Source {

    private static final Pattern SOURCE_FULL_PATH_PATTERN = Pattern.compile("(.*)/dropfolder/([a-zA-Z0-9]{8})/" +
            "(Projects/[a-zA-Z0-9_]+/Source/[a-zA-Z0-9_]+/)");

    @JsonProperty("source_id")
    private String sourceId;

    @JsonProperty("source_display_name")
    private String sourceDisplayName;

    @JsonProperty("relative_path")
    private String relativePath;

    // The dropfolder full path: {bucket}/dropfolder/{dropbox}/Project/{projectId}/Source/{sourceId}/drop/
    @JsonProperty("full_path")
    private String dropFullPath;

    // The source full path: {bucket}/dropfolder/{dropbox}/Project/{projectId}/Source/{sourceId}/
    @JsonIgnore
    private String sourceFullPath;

    @JsonProperty("import_status")
    private DataFeedTask.S3ImportStatus importStatus;

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getSourceDisplayName() {
        return sourceDisplayName;
    }

    public void setSourceDisplayName(String sourceDisplayName) {
        this.sourceDisplayName = sourceDisplayName;
    }

    public String getRelativePath() {
        return relativePath;
    }

    public void setRelativePath(String relativePath) {
        this.relativePath = relativePath;
    }

    public String getDropFullPath() {
        return dropFullPath;
    }

    public void setDropFullPath(String dropFullPath) {
        this.dropFullPath = dropFullPath;
    }

    public String getSourceFullPath() {
        return sourceFullPath;
    }

    public void setSourceFullPath(String sourceFullPath) {
        this.sourceFullPath = sourceFullPath;
    }

    @JsonIgnore
    public String getRelativePathUnderDropfolder() {
        if (StringUtils.isEmpty(sourceFullPath)) {
            return null;
        }
        Matcher matcher = SOURCE_FULL_PATH_PATTERN.matcher(sourceFullPath);
        if (matcher.find()) {
            return matcher.group(3);
        } else {
            return null;
        }
    }

    public DataFeedTask.S3ImportStatus getImportStatus() {
        return importStatus;
    }

    public void setImportStatus(DataFeedTask.S3ImportStatus importStatus) {
        this.importStatus = importStatus;
    }
}
