package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "FILE_DOWNLOAD")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FileDownload implements HasPid {

    @JsonIgnore
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("token")
    @Column(name = "TOKEN", nullable = false)
    private String token;

    @JsonProperty("fileDownloadConfig")
    @Column(name = "FILE_DOWNLOAD_CONFIG", columnDefinition = "'JSON'")
    @Type(type = "json")
    private FileDownloadConfig fileDownloadConfig;

    @JsonProperty("creation")
    @Column(name = "CREATION", nullable = false)
    private Long creation;

    @JsonProperty("ttl")
    @Column(name = "TTL", nullable = false)
    private int ttl;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public FileDownloadConfig getFileDownloadConfig() {
        return fileDownloadConfig;
    }

    public void setFileDownloadConfig(FileDownloadConfig fileDownloadConfig) {
        this.fileDownloadConfig = fileDownloadConfig;
    }

    public Long getCreation() {
        return creation;
    }

    public void setCreation(Long creation) {
        this.creation = creation;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }
}
