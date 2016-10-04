package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;

public class CharacterizationParameters extends DataFlowParameters {

    private String timestampField;
    private List<String> baseTables;
    private Boolean hasSqlPresence = true;
    private Date timestamp;
    private List<SourceColumn> sourceColumns;
    private String versionKey;
    private String attrKey;
    private String categoryKey;
    private String countKey;
    private String percentKey;
    private List<String> attrs;
    private List<String> categories;
    private List<String> groupKeys;
    private Long totalRecords;
    private String version;

    public String getTimestampField() { return timestampField; }

    public void setTimestampField(String timestampField) { this.timestampField = timestampField; }

    public List<String> getBaseTables() { return baseTables; }

    public void setBaseTables(List<String> baseTables) { this.baseTables = baseTables; }

    public Boolean hasSqlPresence() { return hasSqlPresence; }

    public void setHasSqlPresence(Boolean hasSqlPresence) { this.hasSqlPresence = hasSqlPresence; }

    public Date getTimestamp() { return timestamp; }

    public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }

    public List<SourceColumn> getSourceColumns() { return sourceColumns; }

    public void setSourceColumns(List<SourceColumn> sourceColumns) { this.sourceColumns = sourceColumns; }

    public String getAttrKey() { return attrKey; }
    public void setAttrKey(String attrKey) { this.attrKey =  attrKey; }

    public String getCategoryKey() { return categoryKey; }
    public void setCategoryKey(String categoryKey) { this.categoryKey =  categoryKey; }

    public String getCountKey() { return countKey; }
    public void setCountKey(String countKey) { this.countKey =  countKey; }

    public String getPercentKey() { return percentKey; }
    public void setPercentKey(String percentKey) { this.percentKey =  percentKey; }

    public List<String> getGroupKeys() { return groupKeys; }
    public void setGroupKeys(List<String> groupKeys) { this.groupKeys =  groupKeys; }

    public List<String> getAttrs() { return attrs; }
    public void setAttrs(List<String> attrs) { this.attrs =  attrs; }

    public List<String> getCategories() { return categories; }
    public void setCategories(List<String> categories) { this.categories =  categories; }

    public Long getTotalRecords() { return totalRecords; }
    public void setTotalRecords(Long totalRecords) { this.totalRecords =  totalRecords; }

    public String getVersionKey() { return versionKey; }
    public void setVersionKey(String versionKey) { this.versionKey =  versionKey; }

    public String getVersion() { return version; }
    public void setVersion(String version) { this.version =  version; }
}
