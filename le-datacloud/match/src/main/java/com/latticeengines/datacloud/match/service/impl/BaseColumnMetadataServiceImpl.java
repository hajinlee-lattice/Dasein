package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.MetadataColumn;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.propdata.core.entitymgr.DataCloudVersionEntityMgr;
import com.newrelic.api.agent.Trace;

public abstract class BaseColumnMetadataServiceImpl<E extends MetadataColumn> implements ColumnMetadataService {

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Value("${datacloud.match.latest.data.cloud.version:2.0.0}")
    private String latestDataCloudVersion;

    private static final Log log = LogFactory.getLog(BaseColumnMetadataServiceImpl.class);

    private ConcurrentMap<Predefined, List<ColumnMetadata>> predefinedMetaDataCache = new ConcurrentHashMap<>();

    @Autowired
    @Qualifier("taskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @PostConstruct
    private void postConstruct() {
        loadCache();
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                loadCache();
            }
        }, TimeUnit.MINUTES.toMillis(1));
    }

    @Override
    public List<ColumnMetadata> fromPredefinedSelection(Predefined predefined, String dataCloudVersion) {
        String latestVersion = versionEntityMgr.latestApprovedForMajorVersion(latestDataCloudVersion).getVersion();
        if (latestVersion.equals(dataCloudVersion)) {
            return predefinedMetaDataCache.get(predefined);
        } else {
            return fromMetadataColumnService(predefined, dataCloudVersion);
        }
    }

    @Override
    @Trace
    public List<ColumnMetadata> fromSelection(ColumnSelection selection, String dataCloudVersion) {
        List<E> metadataColumns = new ArrayList<>();
        for (Column column : selection.getColumns()) {
            metadataColumns.add(getMetadataColumnService().getMetadataColumn(column.getExternalColumnId(),
                    dataCloudVersion));
        }
        List<ColumnMetadata> metadatas = toColumnMetadata(metadataColumns);
        for (int i = 0; i < metadatas.size(); i++) {
            ColumnMetadata metadata = metadatas.get(i);
            String overwrittenName = selection.getColumns().get(i).getColumnName();
            if (StringUtils.isNotEmpty(overwrittenName)) {
                metadata.setColumnName(overwrittenName);
            } else if (StringUtils.isEmpty(metadata.getColumnName())) {
                throw new IllegalArgumentException(String.format("Cannot find column name for column No.%d", i));
            }
        }
        return metadatas;
    }

    abstract protected MetadataColumnService<E> getMetadataColumnService();

    private List<ColumnMetadata> fromMetadataColumnService(Predefined selectionName, String dataCloudVersion) {
        List<E> columns = getMetadataColumnService().findByColumnSelection(selectionName, dataCloudVersion);
        return toColumnMetadata(columns);
    }

    private List<ColumnMetadata> toColumnMetadata(List<E> columns) {
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        for (MetadataColumn column : columns) {
            try {
                ColumnMetadata columnMetadata = column.toColumnMetadata();
                columnMetadataList.add(columnMetadata);
            } catch (Exception e) {
                throw new RuntimeException("Failed to extract metadata from MetadataColumn [" + column.getColumnId()
                        + "]", e);
            }
        }
        return columnMetadataList;
    }

    @Override
    public Schema getAvroSchema(Predefined selectionName, String recordName, String dataCloudVersion) {
        List<ColumnMetadata> columnMetadatas = fromPredefinedSelection(selectionName, dataCloudVersion);
        return getAvroSchemaFromColumnMetadatas(columnMetadatas, recordName, dataCloudVersion);
    }

    @Override
    public Schema getAvroSchemaFromColumnMetadatas(List<ColumnMetadata> columnMetadatas, String recordName,
            String dataCloudVersion) {
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(recordName);
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        SchemaBuilder.FieldBuilder<Schema> fieldBuilder;
        for (ColumnMetadata columnMetadata : columnMetadatas) {
            String fieldName = columnMetadata.getColumnName();
            fieldBuilder = fieldAssembler.name(StringUtils.strip(fieldName));
            fieldBuilder = fieldBuilder.prop("Tags", "[External]");
            fieldBuilder = fieldBuilder.prop("ApprovedUsage", columnMetadata.getApprovedUsageString());
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
            fieldBuilder = fieldBuilder.prop("Nullable", "true");
            Schema.Type type = getAvroTypeDataType(columnMetadata);
            AvroUtils.constructFieldWithType(fieldAssembler, fieldBuilder, type);
        }
        return fieldAssembler.endRecord();
    }

    private Schema.Type getAvroTypeDataType(ColumnMetadata columnMetadata) {
        String javaClass = columnMetadata.getJavaClass();
        if (StringUtils.isNotEmpty(javaClass)) {
            try {
                return AvroUtils.getAvroType(javaClass);
            } catch (Exception e) {
                log.error("Cannot parse avro type by java class " + javaClass, e);
            }
        }

        String dataType = columnMetadata.getDataType();
        if (StringUtils.isEmpty(dataType)) {
            return null;
        }

        try {
            return AvroUtils.convertSqlTypeToAvro(dataType);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert to avro type from sql server data type " + dataType, e);
        }
    }

    private void loadCache() {
        String latestVersion = versionEntityMgr.latestApprovedForMajorVersion(latestDataCloudVersion).getVersion();
        log.info("Start loading black and white column caches for version " + latestVersion);

        for (Predefined selection : Predefined.values()) {
            try {
                predefinedMetaDataCache.put(selection, fromMetadataColumnService(selection, latestVersion));
            } catch (Exception e) {
                log.error("Failed to load Cache! Type=" + selection, e);
            }
        }
    }

}
