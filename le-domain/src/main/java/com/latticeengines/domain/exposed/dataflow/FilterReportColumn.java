package com.latticeengines.domain.exposed.dataflow;

import java.util.List;

public class FilterReportColumn extends ReportColumn {

    public String expression;

    public List<String> expressionFields;

    public FilterReportColumn(String columnName, String expression, List<String> expressionFields,
            String aggregationType) {
        super(columnName, aggregationType);
        this.expression = expression;
        this.expressionFields = expressionFields;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public FilterReportColumn() {
    }
}
