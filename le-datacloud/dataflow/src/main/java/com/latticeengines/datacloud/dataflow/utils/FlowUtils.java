package com.latticeengines.datacloud.dataflow.utils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.StringTruncateFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.DateTimeCleanupFunction;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Function;

public class FlowUtils {

    private static Logger log = LoggerFactory.getLogger(FlowUtils.class);
    private static Pattern varcharPattern = Pattern.compile("(?<=VARCHAR\\().*?(?=\\))");

    @SuppressWarnings("rawtypes")
    public static Node truncateStringFields(Node node, List<SourceColumn> columns) {
        Set<String> fieldsInMetadata = new HashSet<>(node.getFieldNames());
        for (SourceColumn column : columns) {
            if (fieldsInMetadata.contains(column.getColumnName())
                    && column.getColumnType().toUpperCase().contains("VARCHAR")) {
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
                                node = node.apply(function, new FieldList(column.getColumnName()), new FieldMetadata(
                                        column.getColumnName(), String.class));
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

    public static Node removeInvalidDatetime(Node node, List<SourceColumn> columns) {
        Set<String> fieldsInMetadata = new HashSet<>(node.getFieldNames());
        for (SourceColumn column : columns) {
            if (fieldsInMetadata.contains(column.getColumnName())
                    && column.getColumnType().toUpperCase().contains("DATETIME")) {
                log.info("Cleaning up datetime field " + column.getColumnName());
                DateTimeCleanupFunction function = new DateTimeCleanupFunction(column.getColumnName());
                node = node.apply(function, new FieldList(column.getColumnName()),
                        new FieldMetadata(column.getColumnName(), Long.class));
            }
        }
        return node;
    }

}
