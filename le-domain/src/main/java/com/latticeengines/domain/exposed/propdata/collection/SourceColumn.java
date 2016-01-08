package com.latticeengines.domain.exposed.propdata.collection;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "SourceColumn")
public class SourceColumn implements HasPid, Serializable {

    private static final long serialVersionUID = 5143418326245069058L;
    @Id
    @Column(name = "SourceColumnID", unique = true, nullable = false)
    private Long SourceColumnId;

    @Column(name = "SourceName", nullable = false)
    private String sourceName;

    @Column(name = "ColumnName", nullable = false)
    private String columnName;

    @Column(name = "ColumnType", nullable = false)
    private String columnType;

    @Column(name = "BaseSource", nullable = true)
    private String baseSource;

    @Column(name = "GroupBy", nullable = true)
    private String groupBy;

    @Enumerated(EnumType.STRING)
    @Column(name = "Calculation", nullable = false)
    private Calculation calculation;

    @Column(name = "Arguments", nullable = true)
    private String arguments;

    @Column(name = "Priority", nullable = false)
    private Integer priority;

    @Column(name = "Groups", nullable = false)
    private String groups = "";

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

    public String getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(String groupBy) {
        this.groupBy = groupBy;
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

    public String getGroups() { return groups; }

    public void setGroups(String groups) { this.groups = groups == null ? "" : groups; }

    @Override
    public Long getPid() { return getSourceColumnId(); }

    @Override
    public void setPid(Long pid) { setSourceColumnId(pid); }

    public enum Calculation {
        GROUPBY,
        PIVOT_ANY,
        PIVOT_MAX,
        PIVOT_MIN,
        PIVOT_SUM,
        PIVOT_COUNT,
        PIVOT_EXISTS,
        OTHER,
        BUILTWITH_TOPATTR;

        public static Set<Calculation> unconfigurableCalculations() {
            return new HashSet<>(Arrays.asList(GROUPBY, OTHER, BUILTWITH_TOPATTR));
        }

    }
}