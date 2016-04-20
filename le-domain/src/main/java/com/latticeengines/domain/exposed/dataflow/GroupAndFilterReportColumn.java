package com.latticeengines.domain.exposed.dataflow;

import java.util.List;

public class GroupAndFilterReportColumn extends FilterReportColumn {
    public List<String> groupByFields;

    public GroupAndFilterReportColumn(String columnName, List<String> groupByFields, String filter, List<String> fields,
                                      String aggregationType) {
        super(columnName, filter, fields, aggregationType);
        this.groupByFields = groupByFields;
    }
}
