package com.latticeengines.serviceflows.dataflow.util;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;
import com.latticeengines.dataflow.exposed.builder.Node;

public class DataFlowUtils {
    public static Node normalizeDomain(Node last, String fieldName) {
        return normalizeDomain(last, fieldName, fieldName);
    }

    public static Node normalizeDomain(Node last, String fieldName, String outputFieldName) {
        final String normalizeDomain = "%s != null ? %s.replaceAll(\"^http://\", \"\").replaceAll(\"^www[.]\", \"\").replaceAll(\"/.*$\", \"\") : null";
        final String replaceNulls = "%s != null && %s.equals(\"NULL\") ? null : %s";
        final String toUpperCase = "%s != null ? %s.toUpperCase() : %s";
        return last
                .addFunction(String.format(normalizeDomain, fieldName, fieldName), new FieldList(fieldName),
                        new FieldMetadata(outputFieldName, String.class))
                .addFunction(String.format(replaceNulls, outputFieldName, outputFieldName, outputFieldName),
                        new FieldList(outputFieldName), new FieldMetadata(outputFieldName, String.class))
                .addFunction(String.format(toUpperCase, outputFieldName, outputFieldName, outputFieldName),
                        new FieldList(outputFieldName), new FieldMetadata(outputFieldName, String.class));
    }

    public static Node extractDomainFromEmail(Node last, String emailFieldName, String outputFieldName) {
        final String extract = "%s != null ? %s.replaceAll(\"^.*@\", \"\") : %s";
        return last //
                .addFunction(String.format(extract, emailFieldName, emailFieldName, emailFieldName), //
                        new FieldList(emailFieldName), //
                        new FieldMetadata(outputFieldName, String.class));
    }
}
