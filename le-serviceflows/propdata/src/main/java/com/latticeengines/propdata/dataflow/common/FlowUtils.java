package com.latticeengines.propdata.dataflow.common;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.operation.Function;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.runtime.cascading.StringTruncateFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.DateTimeCleanupFunction;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;

public class FlowUtils {

    private static Log log = LogFactory.getLog(FlowUtils.class);
    private static Pattern varcharPattern = Pattern.compile("(?<=VARCHAR\\().*?(?=\\))");

    public static CascadingDataFlowBuilder.Node truncateStringFields(CascadingDataFlowBuilder.Node node, List<SourceColumn> columns) {
        Set<String> fieldsInMetadata = new HashSet<>(node.getFieldNames());
        for (SourceColumn column : columns) {
            if (fieldsInMetadata.contains(column.getColumnName()) &&
                    column.getColumnType().toUpperCase().contains("VARCHAR")) {
                try {
                    String str = column.getColumnType().toUpperCase();
                    Matcher matcher = varcharPattern.matcher(str);
                    if (matcher.find()) {
                        String matched = matcher.group(0);
                        if (!"MAX".equals(matched)) {
                            Integer length = Integer.valueOf(matched);
                            if (length > 10) {
                                log.info("Truncating field " + column.getColumnName() + " to length " + length);
                                Function function = new StringTruncateFunction(column.getColumnName(), length);
                                node = node.apply(function, new DataFlowBuilder.FieldList(column.getColumnName()),
                                        new DataFlowBuilder.FieldMetadata(column.getColumnName(), String.class));
                            }
                        }
                    } else {
                        throw new RuntimeException("Cannot find pattern " + varcharPattern + " in string " + str);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Cannot parse field length from a string column type "
                            + column.getColumnType());
                }
            }
        }
        return node;
    }

    public static CascadingDataFlowBuilder.Node removeInvalidDatetime(CascadingDataFlowBuilder.Node node, List<SourceColumn> columns) {
        Set<String> fieldsInMetadata = new HashSet<>(node.getFieldNames());
        for (SourceColumn column : columns) {
            if (fieldsInMetadata.contains(column.getColumnName()) &&
                    column.getColumnType().toUpperCase().contains("DATETIME")) {
                log.info("Cleaning up datetime field " + column.getColumnName());
                DateTimeCleanupFunction function = new DateTimeCleanupFunction(column.getColumnName());
                node = node.apply(function, new DataFlowBuilder.FieldList(column.getColumnName()),
                        new DataFlowBuilder.FieldMetadata(column.getColumnName(), Long.class));
            }
        }
        return node;
    }

}
