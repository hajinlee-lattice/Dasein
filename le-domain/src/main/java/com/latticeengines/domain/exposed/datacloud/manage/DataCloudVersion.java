package com.latticeengines.domain.exposed.datacloud.manage;


import java.io.Serializable;
import java.util.Comparator;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "DataCloudVersion")
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataCloudVersion implements HasPid, Serializable {

    private static final long serialVersionUID = -6472245305360293747L;

	@Id
    @JsonIgnore
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("Version")
    @Index(name = "IX_VERSION")
    @Column(name = "Version", nullable = false, unique = true, length = 10)
    private String version;

    @JsonProperty("MajorVersion")
    @Index(name = "IX_MAJOR")
    @Column(name = "MajorVersion", nullable = false, length = 10)
    private String majorVersion;

    @JsonProperty("RefreshVersion")
    @Column(name = "RefreshVersion", nullable = false, length = 100)
    private String refreshVersion;

    @JsonProperty("CreateDate")
    @Column(name = "CreateDate", nullable = false)
    private Date createDate;

    @JsonIgnore
    @Column(name = "AccountMasterHdfsVersion", length = 100)
    private String accountMasterHdfsVersion;

    @JsonIgnore
    @Column(name = "AccountLookupHdfsVersion", length = 100)
    private String accountLookupHdfsVersion;

    @JsonIgnore
    @Column(name = "EnrichmentStatsVersion", length = 100)
    private String enrichmentStatsVersion;

    @JsonIgnore
    @Column(name = "DynamoTableSignature", length = 100)
    private String dynamoTableSignature;

    @JsonIgnore
    @Column(name = "DynamoTableSignature_Lookup", length = 100)
    private String dynamoTableSignatureLookup;

    @JsonProperty("Status")
    @Enumerated(EnumType.STRING)
    @Column(name = "Status", nullable = false)
    private Status status;

    @JsonProperty("Mode")
    @Enumerated(EnumType.STRING)
    @Column(name = "Mode", nullable = false)
    private Mode mode;

    @JsonProperty("MetadataRefreshDate")
    @Column(name = "MetadataRefreshDate", nullable = false)
    private Date metadataRefreshDate;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getAccountMasterHdfsVersion() {
        return accountMasterHdfsVersion;
    }

    public void setAccountMasterHdfsVersion(String accountMasterHdfsVersion) {
        this.accountMasterHdfsVersion = accountMasterHdfsVersion;
    }

    public String getAccountLookupHdfsVersion() {
        return accountLookupHdfsVersion;
    }

    public void setAccountLookupHdfsVersion(String accountLookupHdfsVersion) {
        this.accountLookupHdfsVersion = accountLookupHdfsVersion;
    }

    public String getMajorVersion() {
        return majorVersion;
    }

    public void setMajorVersion(String majorVersion) {
        this.majorVersion = majorVersion;
    }

    public String getRefreshVersionVersion() {
        return refreshVersion;
    }

    public void setRefreshVersion(String refreshVersion) {
        this.refreshVersion = refreshVersion;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getDynamoTableSignature() {
        return dynamoTableSignature;
    }

    public void setDynamoTableSignature(String dynamoTableSignature) {
        this.dynamoTableSignature = dynamoTableSignature;
    }

    public String getDynamoTableSignatureLookup() {
        return dynamoTableSignatureLookup;
    }

    public void setDynamoTableSignatureLookup(String dynamoTableSignatureLookup) {
        this.dynamoTableSignatureLookup = dynamoTableSignatureLookup;
    }

    public String getEnrichmentStatsVersion() {
        return enrichmentStatsVersion;
    }

    public void setEnrichmentStatsVersion(String enrichmentStatsVersion) {
        this.enrichmentStatsVersion = enrichmentStatsVersion;
    }

    public Date getMetadataRefreshDate() {
        return metadataRefreshDate;
    }

    public void setMetadataRefreshDate(Date metadataRefreshDate) {
        this.metadataRefreshDate = metadataRefreshDate;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public enum Status {
        NEW, APPROVED, DEPRECATED
    }

    public enum Mode {
        FULL, MINI
    }

    public String getDataCloudBuildNumber() {
        if (StringUtils.isBlank(refreshVersion)) {
            return version;
        } else {
            return version + "." + refreshVersion;
        }
    }

    public static String parseMajorVersion(String version) {
        String[] tokens = version.split("\\.");
        if (tokens.length < 2) {
            throw new RuntimeException("Cannot parse a major version from " + version);
        }
        return tokens[0] + "." + tokens[1];
    }

    public static String parseMinorVersion(String version) {
        String[] tokens = version.split("\\.");
        if (tokens.length < 3) {
            throw new RuntimeException("Cannot parse a minor version from " + version);
        }
        return tokens[2];
    }

    public static DataCloudVersion parseBuildNumber(String buildNumber) {
        if (buildNumber == null) {
            throw new IllegalArgumentException("Cannot parse null build number");
        }

        Pattern pattern = Pattern.compile("^(\\d+).(\\d+).(\\d+).?(\\d+)?$");
        Matcher matcher = pattern.matcher(buildNumber);

        if (!matcher.find()) {
            throw new IllegalArgumentException("Malformed build number: " + buildNumber);
        }

        String x = matcher.group(1);
        String y = matcher.group(2);
        String z = matcher.group(3);
        String r = matcher.group(3);

        DataCloudVersion version = new DataCloudVersion();
        version.setVersion(String.format("%s.%s.%s", x, y, z));
        version.setRefreshVersion(r);
        version.setMajorVersion(String.format("%s.%s", x, y));

        return version;
    }

    public static Comparator<DataCloudVersion> versionComparator = new Comparator<DataCloudVersion>() {

        public int compare(DataCloudVersion dc1, DataCloudVersion dc2) {
            if (!dc1.majorVersion.equals(dc2.majorVersion)) {
                return dc1.majorVersion.compareTo(dc2.majorVersion);
            }
            String dc1MinorVer = parseMinorVersion(dc1.getVersion());
            String dc2MinorVer = parseMinorVersion(dc2.getVersion());
            return Integer.valueOf(dc1MinorVer).compareTo(Integer.valueOf(dc2MinorVer));
        }

    };

}
