package com.latticeengines.domain.exposed.datacloud.manage;

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
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Index;

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
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@Entity
@Access(AccessType.FIELD)
@Table(name = "AccountMasterColumn", uniqueConstraints = {@UniqueConstraint(columnNames = { "AMColumnID", "DataCloudVersion" })})
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccountMasterColumn implements HasPid, Serializable, MetadataColumn {

    private static final long serialVersionUID = -7516382374246940122L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "AMColumnID", nullable = false, length = 64)
    private String amColumnId;

    @Index(name = "IX_VERSION")
    @Column(name = "DataCloudVersion", nullable = false, length = 50)
    private String dataCloudVersion;

    @Column(name = "DisplayName", nullable = false)
    private String displayName;

    @Column(name = "Description", length = 1000)
    private String description;

    @Column(name = "JavaClass", nullable = false, length = 50)
    private String javaClass;

    @Enumerated(EnumType.STRING)
    @Column(name = "Category", nullable = false, length = 50)
    private Category category;

    @Column(name = "Subcategory", length = 200)
    private String subcategory;

    @Enumerated(EnumType.STRING)
    @Column(name = "StatisticalType", length = 50)
    private StatisticalType statisticalType;

    @Enumerated(EnumType.STRING)
    @Column(name = "FundamentalType", length = 50)
    private FundamentalType fundamentalType;

    @Column(name = "ApprovedUsage")
    private String approvedUsage;

    @Column(name = "Groups", nullable = false, length = 1000)
    private String groups;

    @Column(name = "IsPremium", nullable = false)
    private boolean isPremium;

    @Column(name = "DisplayDiscretizationStrategy", length = 1000)
    private String discretizationStrategy;

    @Column(name = "DecodeStrategy", length = 1000)
    private String decodeStrategy;

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

    @Transient
    @Override
    public String getColumnId() {
        return amColumnId;
    }

    @JsonProperty("AMColumnID")
    public void setAmColumnId(String amColumnId) {
        this.amColumnId = amColumnId;
    }

    @JsonIgnore
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    @JsonIgnore
    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
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

    @Override
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
    public String getSubcategory() {
        return subcategory;
    }

    @JsonIgnore
    public void setSubcategory(String subcategory) {
        this.subcategory = subcategory;
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
    public String getDecodeStrategy() {
        return decodeStrategy;
    }

    @JsonIgnore
    public void setDecodeStrategy(String decodeStrategy) {
        this.decodeStrategy = decodeStrategy;
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
        if (StringUtils.isEmpty(approvedUsage)) {
            return approvedUsages;
        }
        List<String> tokens = Arrays.asList(approvedUsage.split(","));
        for (String token : tokens) {
            approvedUsages.add(ApprovedUsage.fromName(token));
        }
        return approvedUsages;
    }

    @JsonIgnore
    private String getMatchDestination() {
        if (getGroups().contains(ColumnSelection.Predefined.LeadEnrichment.name())) {
            return isPremium() ? "HGData_Pivoted_Source" : "BuiltWith_Pivoted_Source";
        } else {
            return null;
        }
    }

    @Override
    public ColumnMetadata toColumnMetadata() {
        ColumnMetadata metadata = new ColumnMetadata();
        metadata.setColumnId(getAmColumnId());
        metadata.setColumnName(getAmColumnId());
        metadata.setDescription(getDescription());
        metadata.setJavaClass(getJavaClass());
        metadata.setDataType(getJavaClass());
        metadata.setDisplayName(getDisplayName());
        metadata.setCategory(getCategory());
        metadata.setStatisticalType(getStatisticalType());
        metadata.setFundamentalType(getFundamentalType());
        metadata.setApprovedUsageList(getApprovedUsageList());
        metadata.setTagList(Collections.singletonList(Tag.EXTERNAL));
        metadata.setDiscretizationStrategy(getDiscretizationStrategy());
        metadata.setIsPremium(isPremium());
        metadata.setMatchDestination(getMatchDestination());
        return metadata;
    }

}
