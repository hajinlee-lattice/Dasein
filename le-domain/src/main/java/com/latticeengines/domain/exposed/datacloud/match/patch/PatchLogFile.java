package com.latticeengines.domain.exposed.datacloud.match.patch;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Entity class contains the info about the log file of one DataCloud patch result.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PatchLogFile {
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ssZ";

    @JsonProperty("Path")
    private String path;

    @JsonProperty("Url")
    private String url; // public url to access the file

    @JsonProperty("UploadedAt")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DATE_FORMAT)
    private Date uploadedAt;

    @JsonProperty("UrlExpiredAt")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DATE_FORMAT)
    private Date urlExpiredAt; // when the public url will expire

    public PatchLogFile() {
    }

    public PatchLogFile(String path, String url, Date uploadedAt, Date urlExpiredAt) {
        this.path = path;
        this.url = url;
        this.uploadedAt = uploadedAt;
        this.urlExpiredAt = urlExpiredAt;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Date getUploadedAt() {
        return uploadedAt;
    }

    public void setUploadedAt(Date uploadedAt) {
        this.uploadedAt = uploadedAt;
    }

    public Date getUrlExpiredAt() {
        return urlExpiredAt;
    }

    public void setUrlExpiredAt(Date urlExpiredAt) {
        this.urlExpiredAt = urlExpiredAt;
    }
}
