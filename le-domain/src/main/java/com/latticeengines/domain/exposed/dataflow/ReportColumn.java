package com.latticeengines.domain.exposed.dataflow;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ @JsonSubTypes.Type(value = GroupByDomainReportColumn.class), //
        @JsonSubTypes.Type(value = GroupAndFilterReportColumn.class), //
        @JsonSubTypes.Type(value = FilterReportColumn.class) //
})
public class ReportColumn {

    public String aggregationType;
    public String columnName;

    public ReportColumn(String columnName, String aggregationType) {
        this.aggregationType = aggregationType;
        this.columnName = columnName;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public ReportColumn() {
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
