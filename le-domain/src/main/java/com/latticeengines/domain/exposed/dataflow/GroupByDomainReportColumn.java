package com.latticeengines.domain.exposed.dataflow;

public class GroupByDomainReportColumn extends ReportColumn {
    public GroupByDomainReportColumn(String columnName, String aggregationType) {
        super(columnName, aggregationType);
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public GroupByDomainReportColumn() {
    }
}
