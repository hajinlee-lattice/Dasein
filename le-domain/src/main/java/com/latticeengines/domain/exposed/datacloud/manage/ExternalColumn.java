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

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.RefreshFrequency;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Tag;

@Entity
@Access(AccessType.FIELD)
@Table(name = ExternalColumn.TABLE_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExternalColumn implements HasPid, Serializable, MetadataColumn {

    private static final long serialVersionUID = 6232580467581472718L;

    public static final String TABLE_NAME = "ExternalColumn";
    public static final String PID = "PID";
    public static final String EXTERNAL_COLUMN_ID = "ExternalColumnID";
    public static final String DEFAULT_COLUMN_NAME = "DefaultColumnName";
    public static final String TABLE_PARTITION = "TablePartition";
    public static final String DESCRIPTION = "Description";
    public static final String DATA_TYPE = "DataType";
    public static final String DISPLAY_NAME = "DisplayName";
    public static final String CATEGORY = "Category";
    public static final String SUBCATEGORY = "SubCategory";
    public static final String STATISTICAL_TYPE = "StatisticalType";
    public static final String FUNDAMENTAL_TYPE = "FundamentalType";
    public static final String APPROVED_USAGE = "ApprovedUsage";
    public static final String TAGS = "Tags";
    public static final String MATCH_DESTINATION = "MatchDestination";
    public static final String DISPLAY_DISCRETIZATION_STRATEGY = "DisplayDiscretizationStrategy";

    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = PID, unique = true, nullable = false)
    private Long pid;

    @Id
    @Column(name = EXTERNAL_COLUMN_ID, nullable = false, length = 100)
    private String externalColumnID;

    @Column(name = DEFAULT_COLUMN_NAME, nullable = false, length = 100)
    private String defaultColumnName;

    @Column(name = TABLE_PARTITION, nullable = false, length = 200)
    private String tablePartition;

    @Column(name = DESCRIPTION, nullable = true, length = 1000)
    private String description;

    @Column(name = DATA_TYPE, nullable = false, length = 50)
    private String dataType;

    @Column(name = DISPLAY_NAME, nullable = true)
    private String displayName;

    @Enumerated(EnumType.STRING)
    @Column(name = CATEGORY, nullable = false, length = 50)
    private Category category;

    @Column(name = SUBCATEGORY, nullable = true, length = 200)
    private String subCategory;

    @Enumerated(EnumType.STRING)
    @Column(name = STATISTICAL_TYPE, nullable = true, length = 50)
    private StatisticalType statisticalType;

    @Enumerated(EnumType.STRING)
    @Column(name = FUNDAMENTAL_TYPE, nullable = false, length = 50)
    private FundamentalType fundamentalType;

    @Column(name = APPROVED_USAGE, nullable = true)
    private String approvedUsage;

    @Column(name = TAGS, nullable = true, length = 500)
    private String tags;

    @Column(name = MATCH_DESTINATION, nullable = true, length = 200)
    private String matchDestination;

    @Column(name = DISPLAY_DISCRETIZATION_STRATEGY, nullable = true, length = 1000)
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

    @JsonProperty("ExternalColumnID")
    public String getExternalColumnID() {
        return externalColumnID;
    }

    @JsonProperty("ExternalColumnID")
    public void setExternalColumnID(String externalColumnID) {
        this.externalColumnID = externalColumnID;
    }

    @Override
    @Transient
    public String getColumnId() {
        return externalColumnID;
    }

    @JsonIgnore
    public String getDefaultColumnName() {
        return defaultColumnName;
    }

    @JsonIgnore
    public void setDefaultColumnName(String defaultColumnName) {
        this.defaultColumnName = defaultColumnName;
    }

    @JsonIgnore
    public String getTablePartition() {
        return tablePartition;
    }

    @JsonIgnore
    public void setTablePartition(String tablePartition) {
        this.tablePartition = tablePartition;
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
    public String getDataType() {
        return dataType;
    }

    @JsonIgnore
    public void setDataType(String dataType) {
        this.dataType = dataType;
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
    public String getSubCategory() {
        return subCategory;
    }

    @JsonIgnore
    public void setSubCategory(String subCategory) {
        this.subCategory = subCategory;
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

    // Return comma separated approved usages. Don't recommend for external use.
    // Should use getApprovedUsageList() instead
    @JsonIgnore
    public String getApprovedUsage() {
        return approvedUsage;
    }

    @JsonIgnore
    public void setApprovedUsage(String approvedUsage) {
        this.approvedUsage = approvedUsage;
    }

    // Return comma separated tags. Don't recommend for external use.
    // Should use getTagList() instead
    @JsonIgnore
    public String getTags() {
        return tags;
    }

    @JsonIgnore
    public void setTags(String tags) {
        this.tags = tags;
    }

    @JsonIgnore
    public String getDiscretizationStrategy() {
        return discretizationStrategy;
    }

    @JsonIgnore
    public void setDiscretizationStrategy(String discretizationStrategy) {
        this.discretizationStrategy = discretizationStrategy;
    }

    @JsonIgnore
    public String getMatchDestination() {
        return matchDestination;
    }

    @JsonIgnore
    public void setMatchDestination(String matchDestination) {
        this.matchDestination = matchDestination;
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
    public List<ApprovedUsage> getApprovedUsageList() {
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
    public void setApprovedUsageList(List<ApprovedUsage> approvedUsages) {
        List<String> tokens = new ArrayList<>();
        for (ApprovedUsage usage : approvedUsages) {
            tokens.add(usage.getName());
        }
        this.approvedUsage = StringUtils.join(tokens, ",");
    }

    @JsonProperty("ApprovedUsage")
    private List<String> getApprovedUsageJson() {
        List<String> tokens = new ArrayList<>();
        List<ApprovedUsage> approvedUsages = getApprovedUsageList();
        if (approvedUsages.isEmpty()) {
            return tokens;
        }
        for (ApprovedUsage approvedUsage : approvedUsages) {
            tokens.add(approvedUsage.getName());
        }
        return tokens;
    }

    @JsonProperty("ApprovedUsage")
    private void setApprovedUsageJson(List<String> tokens) {
        List<ApprovedUsage> approvedUsages = new ArrayList<>();
        for (String token : tokens) {
            approvedUsages.add(ApprovedUsage.fromName(token));
        }
        setApprovedUsageList(approvedUsages);
    }

    @JsonIgnore
    public List<String> getTagList() {
        List<String> tags = new ArrayList<>();
        if (StringUtils.isEmpty(this.tags)) {
            return tags;
        }
        tags = Arrays.asList(this.tags.split(","));
        return tags;
    }

    @JsonIgnore
    public void setTagList(List<String> tags) {
        List<String> tokens = new ArrayList<>();
        for (String tag : tags) {
            tokens.add(tag);
        }
        this.tags = StringUtils.join(tokens, ",");
    }

    @SuppressWarnings("deprecation")
    @Override
    public ColumnMetadata toColumnMetadata() {
        ColumnMetadata metadata = new ColumnMetadata();
        metadata.setColumnId(getExternalColumnID());
        metadata.setColumnName(getDefaultColumnName());
        metadata.setDescription(getDescription());
        metadata.setDataType(getDataType());
        metadata.setDisplayName(getDisplayName());
        metadata.setCategory(getCategory());
        metadata.setSubcategory(getSubCategory());
        metadata.setStatisticalType(getStatisticalType());
        metadata.setFundamentalType(getFundamentalType());
        metadata.setApprovedUsageList(getApprovedUsageList());
        metadata.setTagList(Collections.singletonList(Tag.EXTERNAL));
        metadata.setDiscretizationStrategy(getDiscretizationStrategy());
        metadata.setMatchDestination(getMatchDestination());
        if (getExternalColumnID().startsWith("TechIndicator")
                && Category.TECHNOLOGY_PROFILE.equals(getCategory())) {
            metadata.setIsPremium(Boolean.TRUE);
        }

        if (metadata.getDataType() != null && metadata.getDataType().contains("VARCHAR")) {
            metadata.setJavaClass(String.class.getSimpleName());
        }
        // DataCloud 1.0.0 has been retired which will never be refreshed.
        // Hardcode the RefreshFrequency to be RELEASE for safe-guard. Should be
        // of no use.
        metadata.setRefreshFrequency(RefreshFrequency.RELEASE);

        return metadata;
    }

    @Override
    public boolean containsTag(String tag) {
        List<String> tagList = getTagList();
        return tagList != null && tagList.contains(tag);
    }
}
