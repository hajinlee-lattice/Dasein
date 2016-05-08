package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnMapping;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.core.service.SourceService;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.service.ExternalColumnService;

@Component
public class ColumnMetadataServiceImpl implements ColumnMetadataService {

    private static final Log log = LogFactory.getLog(ColumnMetadataServiceImpl.class);

    @Autowired
    private SourceService sourceService;

    @Autowired
    private ExternalColumnService externalColumnService;

    private ConcurrentMap<ColumnSelection.Predefined, List<ColumnMetadata>> predefinedMetaDataCache = new ConcurrentHashMap<>();

    @Autowired
    @Qualifier("matchScheduler")
    private ThreadPoolTaskScheduler matchScheduler;

    @PostConstruct
    private void postConstruct() {
        loadCache();
        matchScheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                loadCache();
            }
        }, TimeUnit.MINUTES.toMillis(1));
    }

    @Override
    public List<ColumnMetadata> fromPredefinedSelection(ColumnSelection.Predefined predefined) {
        try {
            if (ColumnSelection.Predefined.Model.equals(predefined)
                    || ColumnSelection.Predefined.DerivedColumns.equals(predefined)) {
                return predefinedMetaDataCache.get(predefined);
            } else {
                throw new UnsupportedOperationException("Only support selection " + ColumnSelection.Predefined.Model
                        + " and " + ColumnSelection.Predefined.DerivedColumns + " now");
            }
        } catch (Exception e) {
            log.warn("Failed to find metadata for selection " + predefined + " in cache");
            List<ColumnMetadata> metadatas = fromExternalColumnService(predefined);
            predefinedMetaDataCache.put(predefined, metadatas);
            return metadatas;
        }
    }

    private List<ColumnMetadata> fromExternalColumnService(ColumnSelection.Predefined selectionName) {
        List<ExternalColumn> externalColumns = externalColumnService.columnSelection(selectionName);
        return toColumnMetadata(externalColumns);
    }

    private List<ColumnMetadata> toColumnMetadata(List<ExternalColumn> externalColumns) {
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        for (ExternalColumn externalColumn : externalColumns) {
            try {
                ColumnMetadata columnMetadata = externalColumn.toColumnMetadata();
                if (externalColumn.getColumnMappings() != null && !externalColumn.getColumnMappings().isEmpty()) {
                    ColumnMapping maxPriorityCM = Collections.max(externalColumn.getColumnMappings(),
                            new Comparator<ColumnMapping>() {
                                public int compare(ColumnMapping cm1, ColumnMapping cm2) {
                                    return Integer.compare(cm1.getPriority(), cm2.getPriority());
                                }
                            });
                    if (maxPriorityCM.getSourceName() != null) {
                        Source source = sourceService.findBySourceName(maxPriorityCM.getSourceName());
                        HasSqlPresence hasSqlPresence = (HasSqlPresence) source;
                        columnMetadata.setMatchDestination(hasSqlPresence.getSqlMatchDestination());
                    }
                }
                columnMetadataList.add(columnMetadata);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to extract metadata from ExternalColumn [" + externalColumn.getExternalColumnID() + "]",
                        e);
            }
        }
        return columnMetadataList;
    }

    public Schema getAvroSchema(ColumnSelection.Predefined selectionName, String recordName) {
        List<ColumnMetadata> columnMetadatas = fromPredefinedSelection(selectionName);
        return getAvroSchemaFromColumnMetadatas(columnMetadatas, recordName);
    }

    private Schema getAvroSchemaFromColumnMetadatas(List<ColumnMetadata> columnMetadatas, String recordName) {
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(recordName);
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        SchemaBuilder.FieldBuilder<Schema> fieldBuilder;
        for (ColumnMetadata columnMetadata : columnMetadatas) {
            String fieldName = columnMetadata.getColumnName();
            fieldBuilder = fieldAssembler.name(StringUtils.strip(fieldName));
            fieldBuilder = fieldBuilder.prop("Tags", "External");
            fieldBuilder = fieldBuilder.prop("ApprovedUsage", columnMetadata.getApprovedUsageList().get(0).getName());
            if (StringUtils.isNotEmpty(columnMetadata.getDisplayName())) {
                fieldBuilder = fieldBuilder.prop("DisplayName", columnMetadata.getDisplayName());
            }
            if (StringUtils.isNotEmpty(columnMetadata.getDescription())) {
                fieldBuilder = fieldBuilder.prop("Description", columnMetadata.getDescription());
            }
            if (columnMetadata.getCategory() != null) {
                fieldBuilder = fieldBuilder.prop("Category", columnMetadata.getCategory().getName());
            }
            if (columnMetadata.getFundamentalType() != null) {
                fieldBuilder = fieldBuilder.prop("FundamentalType", columnMetadata.getFundamentalType().getName());
            }
            if (columnMetadata.getStatisticalType() != null) {
                fieldBuilder = fieldBuilder.prop("StatisticalType", columnMetadata.getStatisticalType().getName());
            }
            if (columnMetadata.getDiscretizationStrategy() != null) {
                fieldBuilder = fieldBuilder.prop("DiscretizationStrategy", columnMetadata.getDiscretizationStrategy());
            }
            String dataType = columnMetadata.getDataType();
            Schema.Type type = getAvroTypeFromSqlServerDataType(dataType);
            AvroUtils.constructFieldWithType(fieldAssembler, fieldBuilder, type);
        }
        return fieldAssembler.endRecord();
    }

    private static Schema.Type getAvroTypeFromSqlServerDataType(String dataType) {
        if (StringUtils.isEmpty(dataType)) {
            return null;
        }

        if (dataType.toLowerCase().contains("varchar")) {
            return AvroUtils.getAvroType(String.class);
        }

        if ("INT".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Integer.class);
        }

        if ("BIGINT".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Long.class);
        }

        if ("REAL".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Float.class);
        }

        if ("FLOAT".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Double.class);
        }

        if ("BIT".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Boolean.class);
        }

        if ("DATETIME".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Long.class);
        }

        if ("DATETIME2".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Long.class);
        }

        if ("DATE".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Long.class);
        }

        throw new RuntimeException("Unknown avro type for sql server data type " + dataType);

    }

    private void loadCache() {
        for (ColumnSelection.Predefined selection : ColumnSelection.Predefined.supportedSelections) {
            try {
                predefinedMetaDataCache.put(selection, fromExternalColumnService(selection));
            } catch (Exception e) {
                log.error(e);
            }
        }
    }

}
