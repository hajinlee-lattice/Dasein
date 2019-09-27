package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.APPROVED_USAGE;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.CATEGORY;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.DATA_TYPE;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.DEFAULT_COLUMN_NAME;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.DESCRIPTION;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.DISPLAY_DISCRETIZATION_STRATEGY;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.DISPLAY_NAME;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.EXTERNAL_COLUMN_ID;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.FUNDAMENTAL_TYPE;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.MATCH_DESTINATION;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.PID;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.STATISTICAL_TYPE;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.SUBCATEGORY;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.TABLE_PARTITION;
import static com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn.TAGS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;

import reactor.core.publisher.Flux;

@Component("externalColumnService")
public class ExternalColumnServiceImpl extends BaseMetadataColumnServiceImpl<ExternalColumn> {

    @Resource(name = "externalColumnEntityMgr")
    private MetadataColumnEntityMgr<ExternalColumn> externalColumnEntityMgr;

    @Value("${datacloud.match.latest.rts.cache.version:1.0.0}")
    private String latstRtsCache;

    private final ConcurrentMap<String, ConcurrentMap<String, ExternalColumn>> whiteColumnCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentSkipListSet<String>> blackColumnCache = new ConcurrentHashMap<>();

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForRTSBasedMatch(version);
    }

    @Override
    protected MetadataColumnEntityMgr<ExternalColumn> getMetadataColumnEntityMgr() {
        return externalColumnEntityMgr;
    }

    @Override
    protected ConcurrentMap<String, ConcurrentMap<String, ExternalColumn>> getWhiteColumnCache() {
        return whiteColumnCache;
    }

    @Override
    protected ConcurrentMap<String, ConcurrentSkipListSet<String>> getBlackColumnCache() {
        return blackColumnCache;
    }

    @Override
    protected String getLatestVersion() {
        return latstRtsCache;
    }

    @Override
    protected Schema getSchema() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(PID, Long.class));
        schema.add(Pair.of(EXTERNAL_COLUMN_ID, String.class));
        schema.add(Pair.of(DEFAULT_COLUMN_NAME, String.class));
        schema.add(Pair.of(TABLE_PARTITION, String.class));
        schema.add(Pair.of(DESCRIPTION, String.class));
        schema.add(Pair.of(DATA_TYPE, String.class));
        schema.add(Pair.of(DISPLAY_NAME, String.class));
        schema.add(Pair.of(CATEGORY, String.class));
        schema.add(Pair.of(SUBCATEGORY, String.class));
        schema.add(Pair.of(STATISTICAL_TYPE, String.class));
        schema.add(Pair.of(FUNDAMENTAL_TYPE, String.class));
        schema.add(Pair.of(APPROVED_USAGE, String.class));
        schema.add(Pair.of(TAGS, String.class));
        schema.add(Pair.of(MATCH_DESTINATION, String.class));
        schema.add(Pair.of(DISPLAY_DISCRETIZATION_STRATEGY, String.class));
        return AvroUtils.constructSchema(ExternalColumn.TABLE_NAME, schema);
    }

    @Override
    protected List<GenericRecord> toGenericRecords(Schema schema, Flux<ExternalColumn> columns) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        return columns.map(column -> {
            builder.set(PID, column.getPid());
            builder.set(EXTERNAL_COLUMN_ID, column.getExternalColumnID());
            builder.set(DEFAULT_COLUMN_NAME, column.getDefaultColumnName());
            builder.set(TABLE_PARTITION, column.getTablePartition());
            builder.set(DESCRIPTION, column.getDescription());
            builder.set(DATA_TYPE, column.getDataType());
            builder.set(DISPLAY_NAME, column.getDisplayName());
            builder.set(CATEGORY, column.getCategory() == null ? null : column.getCategory().name());
            builder.set(SUBCATEGORY, column.getSubCategory());
            builder.set(STATISTICAL_TYPE,
                    column.getStatisticalType() == null ? null : column.getStatisticalType().name());
            builder.set(FUNDAMENTAL_TYPE,
                    column.getFundamentalType() == null ? null : column.getFundamentalType().name());
            builder.set(APPROVED_USAGE, column.getApprovedUsage());
            builder.set(TAGS, column.getTags());
            builder.set(MATCH_DESTINATION, column.getMatchDestination());
            builder.set(DISPLAY_DISCRETIZATION_STRATEGY, column.getDiscretizationStrategy());
            return (GenericRecord) builder.build();
        }).collectList().block();
    }

    protected ExternalColumn toMetadataColumn(GenericRecord record) {
        ExternalColumn column = new ExternalColumn();
        column.setPid((Long) record.get(PID));
        column.setExternalColumnID(AvroUtils.getString(record, EXTERNAL_COLUMN_ID));
        column.setDefaultColumnName(AvroUtils.getString(record, DEFAULT_COLUMN_NAME));
        column.setTablePartition(AvroUtils.getString(record, TABLE_PARTITION));
        column.setDescription(AvroUtils.getString(record, DESCRIPTION));
        column.setDataType(AvroUtils.getString(record, DATA_TYPE));
        column.setDisplayName(AvroUtils.getString(record, DISPLAY_NAME));
        column.setCategory(Category.fromName(AvroUtils.getString(record, CATEGORY)));
        column.setSubCategory(AvroUtils.getString(record, SUBCATEGORY));
        column.setStatisticalType(StatisticalType.fromName(AvroUtils.getString(record, STATISTICAL_TYPE)));
        column.setFundamentalType(FundamentalType.fromName(AvroUtils.getString(record, FUNDAMENTAL_TYPE)));
        column.setApprovedUsage(AvroUtils.getString(record, APPROVED_USAGE));
        column.setTags(AvroUtils.getString(record, TAGS));
        column.setMatchDestination(AvroUtils.getString(record, MATCH_DESTINATION));
        column.setDiscretizationStrategy(AvroUtils.getString(record, DISPLAY_DISCRETIZATION_STRATEGY));
        return column;
    }

    @Override
    protected String getMDSTableName() {
        return ExternalColumn.TABLE_NAME;
    }
}
