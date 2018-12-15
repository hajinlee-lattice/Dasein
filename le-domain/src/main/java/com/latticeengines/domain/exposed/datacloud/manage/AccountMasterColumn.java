package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

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
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

@Entity
@Access(AccessType.FIELD)
@Table(name = "AccountMasterColumn", indexes = { //
        @Index(name = "IX_VERSION", columnList = "DataCloudVersion"), //
        @Index(name = "IX_GROUPS", columnList = "Groups") //
}, uniqueConstraints = { @UniqueConstraint(columnNames = { "AMColumnID", "DataCloudVersion" }) })
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccountMasterColumn implements HasPid, Serializable, MetadataColumn {

    private static final long serialVersionUID = -7516382374246940122L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "AMColumnID", nullable = false, length = 64)
    private String amColumnId;

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

    @Column(name = "Groups", nullable = false, length = 250)
    private String groups;

    @Column(name = "IsInternalEnrichment", nullable = false)
    private boolean internalEnrichment;

    @Column(name = "IsPremium", nullable = false)
    private boolean premium;

    @Column(name = "DisplayDiscretizationStrategy", length = 1000)
    private String discretizationStrategy;

    @Column(name = "DecodeStrategy", length = 1000)
    private String decodeStrategy;

    @Column(name = "IsEOL", nullable = false)
    private boolean eol;

    @Column(name = "DataLicense", length = 100)
    private String dataLicense;

    @Column(name = "EOLVersion", length = 50)
    private String eolVersion;

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

    @Transient
    @Override
    public String getColumnId() {
        return amColumnId;
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
    public String getApprovedUsage() {
        return approvedUsage;
    }

    @JsonIgnore
    public void setApprovedUsage(ApprovedUsage approvedUsage) {
        this.approvedUsage = approvedUsage.getName();
    }

    @JsonIgnore
    public String getGroups() {
        return groups;
    }

    @JsonIgnore
    public void setGroups(String groups) {
        this.groups = groups;
    }

    @JsonIgnore
    public boolean isPremium() {
        return premium;
    }

    @JsonIgnore
    public void setPremium(boolean premium) {
        this.premium = premium;
    }

    @JsonIgnore
    public boolean isInternalEnrichment() {
        return internalEnrichment;
    }

    @JsonIgnore
    public void setInternalEnrichment(boolean internalEnrichment) {
        this.internalEnrichment = internalEnrichment;
    }

    @JsonIgnore
    public boolean isEol() {
        return eol;
    }

    @JsonIgnore
    public void setEol(boolean eol) {
        this.eol = eol;
    }

    @JsonIgnore
    public String getDataLicense() {
        return dataLicense;
    }

    @JsonIgnore
    public void setDataLicense(String dataLicense) {
        this.dataLicense = dataLicense;
    }

    @JsonIgnore
    public String getEolVersion() {
        return eolVersion;
    }

    @JsonIgnore
    public void setEolVersion(String eolVersion) {
        this.eolVersion = eolVersion;
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

    @SuppressWarnings("deprecation")
    @Override
    public ColumnMetadata toColumnMetadata() {
        List<ApprovedUsage> approvedUsages = getApprovedUsageList();

        ColumnMetadata metadata = new ColumnMetadata();
        metadata.setAttrName(getAmColumnId());
        metadata.setDescription(getDescription());
        metadata.setJavaClass(getJavaClass());
        metadata.setDisplayName(getDisplayName());
        metadata.setCategory(getCategory());
        metadata.setSubcategory(getSubcategory());
        metadata.setStatisticalType(getStatisticalType());
        metadata.setFundamentalType(getFundamentalType());
        metadata.setApprovedUsageList(approvedUsages);
        metadata.setTagList(Collections.singletonList(Tag.EXTERNAL));
        metadata.setDiscretizationStrategy(getDiscretizationStrategy());
        metadata.setIsPremium(isPremium());
        metadata.setDecodeStrategy(getDecodeStrategy());
        metadata.setDataLicense(getDataLicense());
        metadata.setCanInternalEnrich(isInternalEnrichment());
        metadata.setGroups(getPredefinedGroups());

        if (StringUtils.isNotBlank(metadata.getDataLicense())
                || Category.WEBSITE_KEYWORDS.equals(metadata.getCategory())) {
            metadata.setAttrState(AttrState.Inactive);
        }

        if (isEol()) {
            metadata.setShouldDeprecate(true);
        }

        if (metadata.isEnabledFor(ColumnSelection.Predefined.Enrichment)) {
            metadata.setCanEnrich(true);
            metadata.enableGroup(ColumnSelection.Predefined.TalkingPoint);
        } else {
            metadata.setCanEnrich(false);
        }

        if (metadata.isEnabledFor(ColumnSelection.Predefined.Segment)) {
            metadata.setCanSegment(true);
            metadata.enableGroup(ColumnSelection.Predefined.Segment);
        } else {
            metadata.setCanSegment(false);
        }

        if (metadata.getApprovedUsageString().contains("Model")) {
            metadata.setCanModel(true);
            metadata.enableGroup(ColumnSelection.Predefined.Model);
        } else {
            metadata.setCanModel(false);
        }

        // deprecated properties
        metadata.setMatchDestination(getMatchDestination());
        metadata.setColumnName(getAmColumnId());
        // remove this type conversion once codescience has started using
        // JavaType instead of SQLServer data types
        metadata.setDataType(JavaToSQLServerDataTypeConverter.convert(getJavaClass()));

        return metadata;
    }

    @JsonIgnore
    private Map<ColumnSelection.Predefined, Boolean> getPredefinedGroups() {
        if (StringUtils.isNotBlank(getGroups())) {
            Map<ColumnSelection.Predefined, Boolean> map = new HashMap<>();
            Arrays.stream(getGroups().split(",")) //
                    .map(ColumnSelection.Predefined::fromName).filter(Objects::nonNull) //
                    .forEach(g -> map.put(g, true));
            return map;
        } else {
            return null;
        }
    }

    // remove this internal class once codescience has started using JavaType
    // instead of SQLServer data types
    static class JavaToSQLServerDataTypeConverter {

        private static final String STRING = "string";
        private static final String DOUBLE = "double";
        private static final String BOOLEAN = "boolean";
        private static final String INTEGER = "integer";
        private static final String LONG = "long";
        private static final String FLOAT = "float";
        private static final String SQL_STRING = "NVARCHAR(MAX)";
        private static final String SQL_FLOAT = "FLOAT";
        private static final String SQL_DECIMAL = "DECIMAL";
        private static final String SQL_BIT = "BIT";
        private static final String SQL_INT = "INT";
        private static final String SQL_BIGINT = "BIGINT";

        public static String convert(String javaClass) {
            String dataType = null;

            if (javaClass != null) {
                switch (javaClass.toLowerCase()) {
                    case STRING:
                        dataType = SQL_STRING;
                        break;
                    case FLOAT:
                        dataType = SQL_FLOAT;
                        break;
                    case DOUBLE:
                        dataType = SQL_DECIMAL;
                        break;
                    case BOOLEAN:
                        dataType = SQL_BIT;
                        break;
                    case INTEGER:
                        dataType = SQL_INT;
                        break;
                    case LONG:
                        dataType = SQL_BIGINT;
                        break;
                    default:
                        dataType = javaClass;
                }
            }

            return dataType;
        }

    }

}
