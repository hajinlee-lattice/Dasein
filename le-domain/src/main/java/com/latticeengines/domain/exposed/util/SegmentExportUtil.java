package com.latticeengines.domain.exposed.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.avro.Schema.Type;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExportType;
import com.latticeengines.domain.exposed.security.Tenant;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SegmentExportUtil {

    private static final Logger log = LoggerFactory.getLogger(SegmentExportUtil.class);

    private static final String DEFAULT_EXPORT_FILE_PREFIX = "unknownsegment";

    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd_HH-mm-ss";

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static Table constructSegmentExportTable(Tenant tenant, MetadataSegmentExportType exportType,
            String displayName, List<Attribute> configuredAccountAttributes,
            List<Attribute> configuredContactAttributes) {

        Map<String, Attribute> combinedAttributes = new HashMap<>();

        exportType.getFieldNamePairs().stream() //
                .map(fieldNamePair -> {
                    Attribute attribute = new Attribute();
                    attribute.setName(fieldNamePair.getKey());
                    attribute.setDisplayName(fieldNamePair.getValue());
                    attribute.setSourceLogicalDataType("");
                    attribute.setPhysicalDataType(Type.STRING.name());
                    return attribute;
                }) //
                .forEach(att -> combinedAttributes.put(att.getName(), att));

        updateCombinedAttributeMap(configuredAccountAttributes, combinedAttributes,
                MetadataSegmentExport.ACCOUNT_PREFIX);
        updateCombinedAttributeMap(configuredContactAttributes, combinedAttributes,
                MetadataSegmentExport.CONTACT_PREFIX);

        Mono<List<Attribute>> stream = Flux.fromIterable(combinedAttributes.keySet()) //
                .map(name -> combinedAttributes.get(name))
                .collectSortedList((a, b) -> a.getName().compareTo(b.getName()));

        log.info(String.format("Combined list of fields for export: %s", combinedAttributes.keySet()));

        List<Attribute> attributes = stream.block();

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

    private static void updateCombinedAttributeMap(List<Attribute> configuredAttributes,
            Map<String, Attribute> combinedAttributes, String prefix) {
        if (CollectionUtils.isNotEmpty(configuredAttributes)) {
            configuredAttributes.stream().map(att -> {
                Attribute attribute = new Attribute();
                attribute.setName(prefix + att.getName());
                attribute.setDisplayName(att.getDisplayName());
                attribute.setSourceLogicalDataType(att.getSourceLogicalDataType());
                attribute.setPhysicalDataType(Type.STRING.name());
                return attribute;
            }).forEach(att -> combinedAttributes.put(att.getName(), att));
        }
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
