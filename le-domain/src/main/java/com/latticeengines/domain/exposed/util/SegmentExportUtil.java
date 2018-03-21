package com.latticeengines.domain.exposed.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.avro.Schema.Type;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExportType;
import com.latticeengines.domain.exposed.security.Tenant;

public class SegmentExportUtil {

    private static final String DEFAULT_EXPORT_FILE_PREFIX = "unknownsegment";

    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd_HH-mm-ss";

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static Table constructSegmentExportTable(Tenant tenant, MetadataSegmentExportType exportType,
            String displayName) {

        List<Attribute> attributes = //
                exportType.getFieldNamePairs().stream() //
                        .map(fieldNamePair -> {
                            Attribute attribute = new Attribute();
                            attribute.setName(fieldNamePair.getKey());
                            attribute.setDisplayName(fieldNamePair.getValue());
                            attribute.setSourceLogicalDataType("");
                            attribute.setPhysicalDataType(Type.STRING.name());
                            return attribute;
                        }) //
                        .collect(Collectors.toList());

        Table segmentExportTable = new Table();
        segmentExportTable.addAttributes(attributes);

        String tableName = "segment_export_" + UUID.randomUUID().toString().replaceAll("-", "_");
        segmentExportTable.setName(tableName);
        segmentExportTable.setTableType(TableType.DATATABLE);

        segmentExportTable.setDisplayName(displayName);
        segmentExportTable.setTenant(tenant);
        segmentExportTable.setTenantId(tenant.getPid());
        segmentExportTable.setMarkedForPurge(false);
        return segmentExportTable;
    }

    public static String constructFileName(String exportPrefix, String segmentDisplayName,
            MetadataSegmentExportType type) {

        String exportedFileName = null;
        if (StringUtils.isNotBlank(exportPrefix)) {
            exportedFileName = exportPrefix;
        } else if (StringUtils.isNotEmpty(segmentDisplayName)) {
            exportedFileName = segmentDisplayName;
        }

        if (StringUtils.isBlank(exportedFileName)) {
            exportedFileName = DEFAULT_EXPORT_FILE_PREFIX;
        }
        exportedFileName = exportedFileName.trim().replaceAll("[^a-zA-Z0-9]", "");

        exportedFileName += "-" + type + "-" + dateFormat.format(new Date()) + "_UTC.csv";
        return exportedFileName;

    }

}
