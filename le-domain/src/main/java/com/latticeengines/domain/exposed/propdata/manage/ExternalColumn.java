package com.latticeengines.domain.exposed.propdata.manage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.propdata.StatisticalType;

@Entity
@Access(AccessType.FIELD)
@Table(name = "ExternalColumn")
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExternalColumn implements HasPid, Serializable {

    private static final long serialVersionUID = 6232580467581472718L;

    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Id
    @Column(name = "ExternalColumnID", nullable = false)
    private String externalColumnID;

    @Column(name = "Description", nullable = false)
    private String description;

    @Column(name = "DataType", nullable = false)
    private String dataType;

    @Column(name = "DisplayName", nullable = true)
    private String displayName;

    @Column(name = "Category", nullable = true)
    private String category;

    @Enumerated(EnumType.STRING)
    @Column(name = "StatisticalType", nullable = true)
    private StatisticalType statisticalType;

    @Enumerated(EnumType.STRING)
    @Column(name = "FundamentalType", nullable = true)
    private FundamentalType fundamentalType;

    @Column(name = "ApprovedUsage", nullable = true)
    private String approvedUsage;

    @Column(name = "Tags", nullable = true)
    private String tags;

    @Column(name = "DisplayDiscretizationStrategy", nullable = true)
    private String discretizationStrategy;

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER, mappedBy = "externalColumn")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<ColumnMapping> columnMappings;

    @Override
    @JsonIgnore
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty("ExternalColumnID")
    public String getExternalColumnID() {
        return externalColumnID;
    }

    @JsonProperty("ExternalColumnID")
    public void setExternalColumnID(String externalColumnID) {
        this.externalColumnID = externalColumnID;
    }

    @JsonProperty("Description")
    public String getDescription() {
        return description;
    }

    @JsonProperty("Description")
    public void setDescription(String description) {
        this.description = description;
    }

    @JsonProperty("DataType")
    public String getDataType() {
        return dataType;
    }

    @JsonProperty("DataType")
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    @JsonProperty("DisplayName")
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("DisplayName")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonProperty("Category")
    public String getCategory() {
        return category;
    }

    @JsonProperty("Category")
    public void setCategory(String category) {
        this.category = category;
    }

    @JsonIgnore
    public StatisticalType getStatisticalType() {
        return statisticalType;
    }

    @JsonIgnore
    public void setStatisticalType(StatisticalType statisticalType) {
        this.statisticalType = statisticalType;
    }

    @JsonIgnore
    public FundamentalType getFundamentalType() {
        return fundamentalType;
    }

    @JsonIgnore
    public void setFundamentalType(FundamentalType fundamentalType) {
        this.fundamentalType = fundamentalType;
    }

    @JsonIgnore
    private String getApprovedUsage() {
        return approvedUsage;
    }

    @JsonIgnore
    private void setApprovedUsage(String approvedUsage) {
        this.approvedUsage = approvedUsage;
    }

    @JsonIgnore
    private String getTags() {
        return tags;
    }

    @JsonIgnore
    private void setTags(String tags) {
        this.tags = tags;
    }

    @JsonProperty("DisplayDiscretizationStrategy")
    public String getDiscretizationStrategy() {
        return discretizationStrategy;
    }

    @JsonProperty("DisplayDiscretizationStrategy")
    public void setDiscretizationStrategy(String discretizationStrategy) {
        this.discretizationStrategy = discretizationStrategy;
    }

    @JsonProperty("ColumnMappings")
    public List<ColumnMapping> getColumnMappings() {
        return columnMappings;
    }

    @JsonProperty("ColumnMappings")
    public void setColumnMappings(List<ColumnMapping> columnMappings) {
        this.columnMappings = columnMappings;
    }

    @JsonProperty("FundamentalType")
    private String getFundamentalTypeAsString() {
        if (fundamentalType == null) {
            return null;
        } else {
            return fundamentalType.getName();
        }
    }

    @JsonProperty("FundamentalType")
    private void setFundamentalTypeByString(String fundamentalType) {
        setFundamentalType(FundamentalType.fromName(fundamentalType));
    }

    @JsonProperty("StatisticalType")
    private String getStatisticalTypeAsString() {
        if (statisticalType == null) {
            return null;
        } else {
            return statisticalType.getName();
        }
    }

    @JsonProperty("StatisticalType")
    private void setStatisticalTypeByString(String statisticalType) {
        setStatisticalType(StatisticalType.fromName(statisticalType));
    }

    @JsonIgnore
    public List<ApprovedUsage> getApprovedUsageList() {
        List<ApprovedUsage> approvedUsages = new ArrayList<>();
        if (StringUtils.isEmpty(approvedUsage)) {  return approvedUsages; }
        List<String> tokens = Arrays.asList(approvedUsage.split(","));
        for (String token: tokens) {
            approvedUsages.add(ApprovedUsage.fromName(token));
        }
        return approvedUsages;
    }

    @JsonIgnore
    public void setApprovedUsageList(List<ApprovedUsage> approvedUsages) {
        List<String> tokens = new ArrayList<>();
        for (ApprovedUsage usage: approvedUsages) {
            tokens.add(usage.getName());
        }
        this.approvedUsage = StringUtils.join(tokens, ",");
    }

    @JsonProperty("ApprovedUsage")
    private List<String> getApprovedUsageJson() {
        List<String> tokens = new ArrayList<>();
        List<ApprovedUsage> approvedUsages = getApprovedUsageList();
        if (approvedUsages.isEmpty()) {  return tokens; }
        for (ApprovedUsage approvedUsage: approvedUsages) {
            tokens.add(approvedUsage.getName());
        }
        return tokens;
    }

    @JsonProperty("ApprovedUsage")
    private void setApprovedUsageJson(List<String> tokens) {
        List<ApprovedUsage> approvedUsages = new ArrayList<>();
        for (String token: tokens) {
            approvedUsages.add(ApprovedUsage.fromName(token));
        }
        setApprovedUsageList(approvedUsages);
    }

    @JsonProperty("Tags")
    public List<String> getTagList() {
        List<String> tags = new ArrayList<>();
        if (StringUtils.isEmpty(this.tags)) {  return tags; }
        tags = Arrays.asList(this.tags.split(","));
        return tags;
    }

    @JsonProperty("Tags")
    public void setTagList(List<String> tags) {
        List<String> tokens = new ArrayList<>();
        for (String tag: tags) {
            tokens.add(tag);
        }
        this.tags = StringUtils.join(tokens, ",");
    }

}
