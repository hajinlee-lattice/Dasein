package com.latticeengines.domain.exposed.propdata.manage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Tag;

@Entity
@Access(AccessType.FIELD)
@Table(name = "AccountMasterColumn")
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccountMasterColumn implements HasPid, Serializable  {

    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Id
    @Column(name = "AMColumnID", nullable = false, length = 100)
    private String amColumnId;

    @Column(name = "DisplayName", nullable = false)
    private String displayName;

    @Column(name = "Description", length = 1000)
    private String description;

    @Column(name = "JavaClass", nullable = false, length = 50)
    private String javaClass;

    @Enumerated(EnumType.STRING)
    @Column(name = "Category", nullable = false, length = 50)
    private Category category;

    @Column(name = "Subcategory", nullable = false, length = 200)
    private String subcategory;

    @Enumerated(EnumType.STRING)
    @Column(name = "StatisticalType", length = 50)
    private StatisticalType statisticalType;

    @Enumerated(EnumType.STRING)
    @Column(name = "FundamentalType", nullable = false, length = 50)
    private FundamentalType fundamentalType;

    @Column(name = "ApprovedUsage")
    private String approvedUsage;

    @Column(name = "Groups", nullable = false, length = 1000)
    private String groups;

    @Column(name = "IsPremium")
    private boolean isPremium;

    @Column(name = "DisplayDiscretizationStrategy", length = 1000)
    private String discretizationStrategy;

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

    @JsonProperty("AMColumnID")
    public String getAmColumnId() {
        return amColumnId;
    }

    @JsonProperty("AMColumnID")
    public void setAmColumnId(String amColumnId) {
        this.amColumnId = amColumnId;
    }

    @JsonIgnore
    public String getDescription() {
        return description;
    }

    @JsonIgnore
    public void setDescription(String description) {
        this.description = description;
    }

    @JsonIgnore
    public String getJavaClass() {
        return javaClass;
    }

    @JsonIgnore
    public void setJavaClass(String javaClass) {
        this.javaClass = javaClass;
    }

    @JsonProperty("DisplayName")
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("DisplayName")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonIgnore
    public Category getCategory() {
        return category;
    }

    @JsonIgnore
    public void setCategory(Category category) {
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
    private String getGroups() {
        return groups;
    }

    @JsonIgnore
    private void setGroups(String groups) {
        this.groups = groups;
    }

    @JsonIgnore
    public boolean isPremium() {
        return isPremium;
    }

    @JsonIgnore
    public void setPremium(boolean premium) {
        isPremium = premium;
    }

    @JsonIgnore
    private String getDiscretizationStrategy() {
        return discretizationStrategy;
    }

    @JsonIgnore
    public void setDiscretizationStrategy(String discretizationStrategy) {
        this.discretizationStrategy = discretizationStrategy;
    }

    @JsonIgnore
    private String getFundamentalTypeAsString() {
        if (fundamentalType == null) {
            return null;
        } else {
            return fundamentalType.getName();
        }
    }

    @JsonIgnore
    private void setFundamentalTypeByString(String fundamentalType) {
        setFundamentalType(FundamentalType.fromName(fundamentalType));
    }

    @JsonIgnore
    private String getStatisticalTypeAsString() {
        if (statisticalType == null) {
            return null;
        } else {
            return statisticalType.getName();
        }
    }

    @JsonIgnore
    private void setStatisticalTypeByString(String statisticalType) {
        setStatisticalType(StatisticalType.fromName(statisticalType));
    }

    @JsonIgnore
    private List<ApprovedUsage> getApprovedUsageList() {
        List<ApprovedUsage> approvedUsages = new ArrayList<>();
        if (StringUtils.isEmpty(approvedUsage)) {  return approvedUsages; }
        List<String> tokens = Arrays.asList(approvedUsage.split(","));
        for (String token: tokens) {
            approvedUsages.add(ApprovedUsage.fromName(token));
        }
        return approvedUsages;
    }

    public ColumnMetadata toColumnMetadata() {
        ColumnMetadata metadata = new ColumnMetadata();
        metadata.setColumnId(getAmColumnId());
        metadata.setColumnName(getAmColumnId());
        metadata.setDescription(getDescription());
        metadata.setJavaClass(getJavaClass());
        metadata.setDisplayName(getDisplayName());
        metadata.setCategory(getCategory());
        metadata.setStatisticalType(getStatisticalType());
        metadata.setFundamentalType(getFundamentalType());
        metadata.setApprovedUsageList(getApprovedUsageList());
        metadata.setTagList(Collections.singletonList(Tag.EXTERNAL));
        metadata.setDiscretizationStrategy(getDiscretizationStrategy());
        metadata.setIsPremium(isPremium());
        return metadata;
    }

}
