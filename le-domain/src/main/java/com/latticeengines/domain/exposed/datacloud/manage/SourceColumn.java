package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.latticeengines.domain.exposed.dataplatform.HasPid;



@Entity
@Access(AccessType.FIELD)
@Table(name = "SourceColumn")
public class SourceColumn implements HasPid, Serializable {

    private static final long serialVersionUID = 5143418326245069058L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "SourceColumnID", unique = true, nullable = false)
    private Long SourceColumnId;

    @Column(name = "SourceName", nullable = false, length = 100)
    private String sourceName;

    @Column(name = "ColumnName", nullable = false, length = 100)
    private String columnName;

    @Column(name = "ColumnType", nullable = true, length = 50)
    private String columnType;

    @Column(name = "BaseSource", length = 100)
    private String baseSource;

    @Column(name = "Preparation", length = 1000)
    private String preparation;

    @Column(name = "GroupBy", length = 100)
    private String groupBy;

    @Column(name = "JoinBy", length = 100)
    private String joinBy;

    @Enumerated(EnumType.STRING)
    @Column(name = "Calculation", nullable = false, length = 50)
    private Calculation calculation;

    @Column(name = "Arguments", length = 3000)
    private String arguments;

    @Column(name = "Priority", nullable = false)
    private Integer priority;

    @Column(name = "Groups", nullable = false)
    private String groups = "";

    @Column(name = "CharAttrId")
    private Integer charAttrId;

    @Column(name = "Categories", length = 1000)
    private String categories;

    @Transient
    private List<String> categoryList;

    public SourceColumn() {
        super();
    }

    public Long getSourceColumnId() {
        return SourceColumnId;
    }

    public void setSourceColumnId(Long sourceColumnId) {
        SourceColumnId = sourceColumnId;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getBaseSource() {
        return baseSource;
    }

    public void setBaseSource(String baseSource) {
        this.baseSource = baseSource;
    }

    public String getPreparation() {
        return preparation;
    }

    public void setPreparation(String preparation) {
        this.preparation = preparation;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(String groupBy) {
        this.groupBy = groupBy;
    }

    public String getJoinBy() {
        return joinBy;
    }

    public void setJoinBy(String joinBy) {
        this.joinBy = joinBy;
    }

    public Calculation getCalculation() {
        return calculation;
    }

    public void setCalculation(Calculation calculation) {
        this.calculation = calculation;
    }

    public String getArguments() {
        return arguments;
    }

    public void setArguments(String arguments) {
        this.arguments = arguments;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public String getGroups() {
        return groups;
    }

    public void setGroups(String groups) {
        this.groups = groups == null ? "" : groups;
    }

    public Integer getCharAttrId() {
        return charAttrId;
    }

    public void setCharAttrId(Integer charAttrId) {
        this.charAttrId = charAttrId;
    }

    private String getCategories() {
        return categories;
    }

    public List<String> getCategoryList() {
        if (categoryList == null) {
            List<String> catList = new ArrayList<>();
            String catStr = getCategories();
            if (StringUtils.isNotEmpty(catStr)) {
                catList = Arrays.asList(catStr.split(","));
            }
            this.categoryList = catList;
        }
        return this.categoryList;
    }

    @Override
    public Long getPid() {
        return getSourceColumnId();
    }

    @Override
    public void setPid(Long pid) {
        setSourceColumnId(pid);
    }

    public enum Calculation {
        GROUPBY, //
        PIVOT_ANY, //
        PIVOT_MAX, //
        PIVOT_MIN, //
        PIVOT_SUM, //
        PIVOT_COUNT, //
        PIVOT_EXISTS, //
        AGG_MIN, //
        AGG_MAX, //
        AGG_SUM, //
        AGG_COUNT, //
        OTHER, //
        BUILTWITH_TOPATTR, //
        HGDATA_NEWTECH, //
        DEPIVOT, //
        COLUMN_NAME_MAPPING, //
        MERGE_SEED, //
        ADD_UUID, //
        ADD_TIMESTAMP, //
        ADD_ROWNUM, //
        STANDARD_DOMAIN, //
        CONVERT_TYPE, //
        BIT_ENCODE;
    }
}
