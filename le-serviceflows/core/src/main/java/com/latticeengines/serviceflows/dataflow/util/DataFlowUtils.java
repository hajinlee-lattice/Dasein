package com.latticeengines.serviceflows.dataflow.util;

import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;

public class DataFlowUtils {
    public static Node extractDomain(Node last, String columnName) {

        if (last.getFieldNames().contains(InterfaceName.Domain.toString())) {
            last = last.rename(new FieldList(InterfaceName.Domain.toString()), new FieldList(columnName));
        } else if (last.getFieldNames().contains(InterfaceName.Website.toString())) {
            last = DataFlowUtils.normalizeDomain(last, InterfaceName.Website.toString(), columnName);
        } else if (last.getFieldNames().contains(InterfaceName.Email.toString())) {
            last = DataFlowUtils.extractDomainFromEmail(last, InterfaceName.Email.toString(), columnName);
            last = DataFlowUtils.normalizeDomain(last, columnName);
        } else {
            throw new RuntimeException("Need a website, domain, or email column");
        }
        return last;
    }

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

    public static Node addInternalId(Node last) {
        if (!hasInternalId(last)) {
            FieldMetadata fm = new FieldMetadata(InterfaceName.InternalId.toString(), Long.class);
            fm.setPropertyValue("logicalType", LogicalDataType.InternalId.toString());
            fm.setPropertyValue("ApprovedUsage", ModelingMetadata.NONE_APPROVED_USAGE);
            fm.setPropertyValue("displayName", "Id");
            last = last.addRowID(fm);
        }
        return last;
    }

    public static boolean hasInternalId(Node last) {
        List<FieldMetadata> fields = last.getSchema();
        return Iterables.any(fields, new Predicate<FieldMetadata>() {
            @Override
            public boolean apply(@Nullable FieldMetadata input) {
                String value = input.getPropertyValue("logicalType");
                return value != null && value.equals(LogicalDataType.InternalId.toString());
            }
        });
    }

    public static Node removeInternalId(Node last) {
        if (hasInternalId(last)) {
            Iterable<FieldMetadata> filtered = Iterables.filter(last.getSchema(), new Predicate<FieldMetadata>() {
                @Override
                public boolean apply(@Nullable FieldMetadata input) {
                    String value = input.getPropertyValue("logicalType");
                    return value != null && value.equals(LogicalDataType.InternalId.toString());
                }
            });
            Iterator<FieldMetadata> iter = filtered.iterator();
            while (iter.hasNext()) {
                last = last.discard(new FieldList(iter.next().getFieldName()));
            }
        }
        return last;
    }
}
