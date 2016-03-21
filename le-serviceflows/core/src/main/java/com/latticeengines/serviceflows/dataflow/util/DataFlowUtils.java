package com.latticeengines.serviceflows.dataflow.util;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;

public class DataFlowUtils {
    public static CascadingDataFlowBuilder.Node normalizeDomain(CascadingDataFlowBuilder.Node last, String fieldName) {
        return normalizeDomain(last, fieldName, fieldName);
    }

    public static CascadingDataFlowBuilder.Node normalizeDomain(CascadingDataFlowBuilder.Node last, String fieldName,
            String outputFieldName) {
        final String normalizeDomain = "%s != null ? %s.replaceAll(\"^http://\", \"\").replaceAll(\"^www[.]\", \"\").replaceAll(\"/.*$\", \"\") : null";
        final String replaceNulls = "%s != null && %s.equals(\"NULL\") ? null : %s";
        final String toUpperCase = "%s != null ? %s.toUpperCase() : %s";
        return last
                .addFunction(String.format(normalizeDomain, fieldName, fieldName),
                        new DataFlowBuilder.FieldList(fieldName),
                        new DataFlowBuilder.FieldMetadata(outputFieldName, String.class))
                .addFunction(String.format(replaceNulls, outputFieldName, outputFieldName, outputFieldName),
                        new DataFlowBuilder.FieldList(outputFieldName),
                        new DataFlowBuilder.FieldMetadata(outputFieldName, String.class))
                .addFunction(String.format(toUpperCase, outputFieldName, outputFieldName, outputFieldName),
                        new DataFlowBuilder.FieldList(outputFieldName),
                        new DataFlowBuilder.FieldMetadata(outputFieldName, String.class));
    }

    public static CascadingDataFlowBuilder.Node extractDomainFromEmail(CascadingDataFlowBuilder.Node last,
            String emailFieldName, String outputFieldName) {
        final String extract = "%s != null ? %s.replaceAll(\"^.*@\", \"\") : %s";
        return last //
                .addFunction(String.format(extract, emailFieldName, emailFieldName, emailFieldName), //
                        new DataFlowBuilder.FieldList(emailFieldName), //
                        new DataFlowBuilder.FieldMetadata(outputFieldName, String.class));
    }

    public static CascadingDataFlowBuilder.Node addInternalId(CascadingDataFlowBuilder.Node last) {
        DataFlowBuilder.FieldMetadata fm = new DataFlowBuilder.FieldMetadata(InterfaceName.InternalId.toString(),
                Long.class);
        fm.setPropertyValue("logicalType", LogicalDataType.InternalId.toString());
        fm.setPropertyValue("ApprovedUsage", ModelingMetadata.NONE_APPROVED_USAGE);
        fm.setPropertyValue("displayName", "Id");
        last = last.addRowID(fm);
        return last;
    }
}
