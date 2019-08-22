package com.latticeengines.domain.exposed.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.kitesdk.shaded.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

import reactor.core.publisher.Flux;

public class SegmentExportUtil {

    private static final Logger log = LoggerFactory.getLogger(SegmentExportUtil.class);

    private static final String DEFAULT_EXPORT_FILE_PREFIX = "unknownsegment";

    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd_HH-mm-ss";

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static String SEPARATOR = "___";

    // For entity match enabled tenant, following attributes need to be hidden
    // from export
    // Just short-term fix. Exporting segment will change to Spark job
    // ExtractAtlasEntity which will only export attributes which are enabled
    // for enrichment (handled by metadata decorator)
    private static final Set<String> EM_HIDE_ATTRS = ImmutableSet.of( //
            InterfaceName.AccountId.name(), InterfaceName.ContactId.name());

    public static Table constructSegmentExportTable(//
            Tenant tenant, MetadataSegmentExport metadataSegmentExportJob, //
            Map<BusinessEntity, List<Attribute>> configuredBusEntityAttrMap, boolean entityMatchEnabled) {

        String tableName = metadataSegmentExportJob.getTableName();
        String displayName = metadataSegmentExportJob.getFileName();

        List<Attribute> combinedAttributes = new ArrayList<>();
        for (Map.Entry<BusinessEntity, List<Attribute>> ent : configuredBusEntityAttrMap.entrySet()) {
            List<Attribute> configuredAttributes = ent.getValue();
            if (entityMatchEnabled) {
                log.info(
                        "Tenant is enabled with entity match. Will hide AccountId/ContactId from exporting data for entity {}",
                        ent.getKey());
                configuredAttributes = configuredAttributes.stream()
                        // some attribute names are renamed to entity___attr in
                        // SegmentExportProcessor
                        .filter(attr -> !EM_HIDE_ATTRS.contains(attr.getName())
                                && (!attr.getName().startsWith(ent.getKey() + SEPARATOR) || !EM_HIDE_ATTRS
                                        .contains(attr.getName().substring((ent.getKey() + SEPARATOR).length()))))
                        .collect(Collectors.toList());
            }
            combineAttributes(configuredAttributes, combinedAttributes);
        }

        log.info(String.format("Combined list of fields for export: %s",
                JsonUtils.serialize(combinedAttributes)));

        Table segmentExportTable = new Table();
        segmentExportTable.addAttributes(combinedAttributes);

        segmentExportTable.setName(tableName);
        segmentExportTable.setTableType(TableType.DATATABLE);

        segmentExportTable.setDisplayName(displayName);
        segmentExportTable.setTenant(tenant);
        segmentExportTable.setMarkedForPurge(false);
        return segmentExportTable;
    }

    private static void combineAttributes(List<Attribute> configuredAttributes,
            List<Attribute> combinedAttributes) {
        if (CollectionUtils.isNotEmpty(configuredAttributes)) {
            combinedAttributes.addAll(Flux.fromIterable(configuredAttributes) //
                    .collectSortedList((a, b) -> a.getName().compareTo(b.getName())).block());
        }
    }

    public static String constructFileName(String exportPrefix, String segmentDisplayName,
            AtlasExportType type) {

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
