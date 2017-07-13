package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.AMRelease;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.MetadataColumn;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.newrelic.api.agent.Trace;

public abstract class BaseColumnMetadataServiceImpl<E extends MetadataColumn>
        implements ColumnMetadataService {

    private static final Logger log = LoggerFactory.getLogger(BaseColumnMetadataServiceImpl.class);

    private WatcherCache<ImmutablePair<String, Predefined>, List<ColumnMetadata>> predefinedMetaDataCache;

    protected abstract String getLatestVersion();

    @PostConstruct
    private void postConstruct() {
        initCache();
    }

    @Override
    public List<ColumnMetadata> fromPredefinedSelection(Predefined predefined,
            String dataCloudVersion) {
        return predefinedMetaDataCache.get(ImmutablePair.of(dataCloudVersion, predefined));
    }

    @Override
    @Trace
    public List<ColumnMetadata> fromSelection(ColumnSelection selection, String dataCloudVersion) {
        List<E> metadataColumns = getMetadataColumnService()
                .getMetadataColumns(selection.getColumnIds(), dataCloudVersion);
        return toColumnMetadata(metadataColumns);
    }

    abstract protected MetadataColumnService<E> getMetadataColumnService();

    private List<ColumnMetadata> fromMetadataColumnService(Predefined selectionName,
            String dataCloudVersion) {
        List<E> columns = getMetadataColumnService().findByColumnSelection(selectionName,
                dataCloudVersion);
        return toColumnMetadata(columns);
    }

    private List<ColumnMetadata> toColumnMetadata(List<E> columns) {
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        for (MetadataColumn column : columns) {
            try {
                ColumnMetadata columnMetadata = column.toColumnMetadata();
                columnMetadataList.add(columnMetadata);
            } catch (Exception e) {
                throw new RuntimeException("Failed to extract metadata from MetadataColumn ["
                        + column.getColumnId() + "]", e);
            }
        }
        return columnMetadataList;
    }

    @Override
    public List<ColumnMetadata> findAll(String dataCloudVersion) {
        return toColumnMetadata(getMetadataColumnService().scan(dataCloudVersion));
    }

    @Override
    public Schema getAvroSchema(Predefined selectionName, String recordName,
            String dataCloudVersion) {
        List<ColumnMetadata> columnMetadatas = fromPredefinedSelection(selectionName,
                dataCloudVersion);
        return getAvroSchemaFromColumnMetadatas(columnMetadatas, recordName, dataCloudVersion);
    }

    @Override
    public Schema getAvroSchemaFromColumnMetadatas(List<ColumnMetadata> columnMetadatas,
            String recordName, String dataCloudVersion) {
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(recordName);
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        SchemaBuilder.FieldBuilder<Schema> fieldBuilder;
        for (ColumnMetadata columnMetadata : columnMetadatas) {
            String fieldName = columnMetadata.getColumnId();
            fieldBuilder = fieldAssembler.name(StringUtils.strip(fieldName));
            fieldBuilder = fieldBuilder.prop("Tags", "[External]");
            fieldBuilder = fieldBuilder.prop("ApprovedUsage",
                    columnMetadata.getApprovedUsageString());
            if (StringUtils.isNotEmpty(columnMetadata.getDisplayName())) {
                fieldBuilder = fieldBuilder.prop("DisplayName", columnMetadata.getDisplayName());
            }
            if (StringUtils.isNotEmpty(columnMetadata.getDescription())) {
                fieldBuilder = fieldBuilder.prop("Description", columnMetadata.getDescription());
            }
            if (columnMetadata.getCategory() != null) {
                fieldBuilder = fieldBuilder.prop("Category",
                        columnMetadata.getCategory().getName());
            }
            if (columnMetadata.getFundamentalType() != null) {
                fieldBuilder = fieldBuilder.prop("FundamentalType",
                        columnMetadata.getFundamentalType().getName());
            }
            if (columnMetadata.getStatisticalType() != null) {
                fieldBuilder = fieldBuilder.prop("StatisticalType",
                        columnMetadata.getStatisticalType().getName());
            }
            if (columnMetadata.getDiscretizationStrategy() != null) {
                fieldBuilder = fieldBuilder.prop("DiscretizationStrategy",
                        columnMetadata.getDiscretizationStrategy());
            }
            fieldBuilder = fieldBuilder.prop("Nullable", "true");
            Schema.Type type = getAvroTypeDataType(columnMetadata);
            AvroUtils.constructFieldWithType(fieldAssembler, fieldBuilder, type);
        }
        return fieldAssembler.endRecord();
    }

    private void validateColumnMetadatas(String dataCloudVersion,
            List<ColumnMetadata> columnMetadatas) {
        for (ColumnMetadata columnMetadata : columnMetadatas) {
            if (Boolean.TRUE.equals(columnMetadata.isCanBis())
                    && (!Boolean.TRUE.equals(columnMetadata.isCanInsights()) || !Boolean.TRUE.equals(columnMetadata.isCanModel()))) {
                throw new LedpException(LedpCode.LEDP_25026,
                        new String[] { columnMetadata.getDisplayName(), dataCloudVersion });
            } else if (Boolean.TRUE.equals(columnMetadata.isCanInsights()) && !Boolean.TRUE.equals(columnMetadata.isCanModel())) {
                throw new LedpException(LedpCode.LEDP_25026,
                        new String[] { columnMetadata.getDisplayName(), dataCloudVersion });
            }
        }
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
            throw new RuntimeException(
                    "Failed to convert to avro type from sql server data type " + dataType, e);
        }
    }

    @SuppressWarnings("unchecked")
    private void initCache() {
        String currentApproved = getLatestVersion();
        List<ImmutablePair<String, Predefined>> predefinedForLatestVersion = Predefined.supportedSelections.stream() //
                .map(p -> ImmutablePair.of(currentApproved, p)).collect(Collectors.toList());
        ImmutablePair<String, Predefined>[] initKeys = predefinedForLatestVersion
                .toArray(new ImmutablePair[predefinedForLatestVersion.size()]);
         predefinedMetaDataCache = WatcherCache.builder() //
                .name("PredefinedSelectionCacheForRTSCache") //
                .watch(AMRelease) //
                .maximum(10) //
                .load(key -> {
                    ImmutablePair<String, Predefined> pair = (ImmutablePair<String, Predefined>) key;
                    return fromMetadataColumnService(pair.getRight(), pair.getLeft());
                }) //
                .initKeys(initKeys) //
                .build();
    }

}
