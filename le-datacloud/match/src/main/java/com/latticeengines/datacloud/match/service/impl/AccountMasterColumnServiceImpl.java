package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.AMCOLUMN_ID;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.APPROVED_USAGE;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.CATEGORY;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.DATACLOUD_VERSION;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.DATA_LICENSE;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.DECODE_STRATEGY;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.DESCRIPTION;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.DISPLAY_DISCRETIZATION_STRATEGY;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.DISPLAY_NAME;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.EOL_VERSION;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.FUNDAMENTAL_TYPE;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.GROUPS;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.IS_EOL;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.IS_INTERNAL_ENRICHMENT;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.IS_PREMIUM;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.JAVA_CLASS;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.PID;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.REFRESH_FREQ;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.STATISTICAL_TYPE;
import static com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn.SUBCATEGORY;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.match.RefreshFrequency;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

import reactor.core.publisher.Flux;

@Component("accountMasterColumnService")
public class AccountMasterColumnServiceImpl extends BaseMetadataColumnServiceImpl<AccountMasterColumn> {

    @Resource(name = "accountMasterColumnEntityMgr")
    private MetadataColumnEntityMgr<AccountMasterColumn> metadataColumnEntityMgr;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    private final ConcurrentMap<String, ConcurrentMap<String, AccountMasterColumn>> whiteColumnCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentSkipListSet<String>> blackColumnCache = new ConcurrentHashMap<>();

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    protected MetadataColumnEntityMgr<AccountMasterColumn> getMetadataColumnEntityMgr() {
        return metadataColumnEntityMgr;
    }

    @Override
    protected ConcurrentMap<String, ConcurrentMap<String, AccountMasterColumn>> getWhiteColumnCache() {
        return whiteColumnCache;
    }

    @Override
    protected ConcurrentMap<String, ConcurrentSkipListSet<String>> getBlackColumnCache() {
        return blackColumnCache;
    }

    @Override
    public List<AccountMasterColumn> findByColumnSelection(Predefined selectName, String dataCloudVersion) {
        List<AccountMasterColumn> columns = getMetadataColumns(dataCloudVersion);
        return columns.stream() //
                .filter(column -> column.containsTag(selectName.getName())) //
                // All the sorting values are actually guaranteed to be not
                // null. Add Comparator.nullsLast for safe-guard
                .sorted(Comparator.nullsLast(Comparator.comparing(AccountMasterColumn::getCategory) //
                        .thenComparing(AccountMasterColumn::getSubcategory) //
                        .thenComparing(AccountMasterColumn::getPid))) //
                .collect(Collectors.toList());
    }

    @Override
    protected String getLatestVersion() {
        return versionEntityMgr.currentApprovedVersionAsString();
    }

    @Override
    protected Schema getSchema() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(PID, Long.class));
        schema.add(Pair.of(AMCOLUMN_ID, String.class));
        schema.add(Pair.of(DATACLOUD_VERSION, String.class));
        schema.add(Pair.of(DISPLAY_NAME, String.class));
        schema.add(Pair.of(DESCRIPTION, String.class));
        schema.add(Pair.of(JAVA_CLASS, String.class));
        schema.add(Pair.of(CATEGORY, String.class));
        schema.add(Pair.of(SUBCATEGORY, String.class));
        schema.add(Pair.of(STATISTICAL_TYPE, String.class));
        schema.add(Pair.of(FUNDAMENTAL_TYPE, String.class));
        schema.add(Pair.of(APPROVED_USAGE, String.class));
        schema.add(Pair.of(GROUPS, String.class));
        schema.add(Pair.of(IS_INTERNAL_ENRICHMENT, Boolean.class));
        schema.add(Pair.of(IS_PREMIUM, Boolean.class));
        schema.add(Pair.of(DISPLAY_DISCRETIZATION_STRATEGY, String.class));
        schema.add(Pair.of(DECODE_STRATEGY, String.class));
        schema.add(Pair.of(IS_EOL, Boolean.class));
        schema.add(Pair.of(DATA_LICENSE, String.class));
        schema.add(Pair.of(EOL_VERSION, String.class));
        schema.add(Pair.of(REFRESH_FREQ, String.class));
        return AvroUtils.constructSchema(AccountMasterColumn.TABLE_NAME, schema);
    }

    @Override
    protected List<GenericRecord> toGenericRecords(Schema schema, Flux<AccountMasterColumn> columns) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        return columns.map(column -> {
            builder.set(PID, column.getPid());
            builder.set(AMCOLUMN_ID, column.getAmColumnId());
            builder.set(DATACLOUD_VERSION, column.getDataCloudVersion());
            builder.set(DISPLAY_NAME, column.getDisplayName());
            builder.set(DESCRIPTION, column.getDescription());
            builder.set(JAVA_CLASS, column.getJavaClass());
            builder.set(CATEGORY, column.getCategory() == null ? null : column.getCategory().name());
            builder.set(SUBCATEGORY, column.getSubcategory());
            builder.set(STATISTICAL_TYPE,
                    column.getStatisticalType() == null ? null : column.getStatisticalType().name());
            builder.set(FUNDAMENTAL_TYPE,
                    column.getFundamentalType() == null ? null : column.getFundamentalType().name());
            builder.set(APPROVED_USAGE, column.getApprovedUsage());
            builder.set(GROUPS, column.getGroups());
            builder.set(IS_INTERNAL_ENRICHMENT, column.isInternalEnrichment());
            builder.set(IS_PREMIUM, column.isPremium());
            builder.set(DISPLAY_DISCRETIZATION_STRATEGY, column.getDiscretizationStrategy());
            builder.set(DECODE_STRATEGY, column.getDecodeStrategy());
            builder.set(IS_EOL, column.isEol());
            builder.set(DATA_LICENSE, column.getDataLicense());
            builder.set(EOL_VERSION, column.getEolVersion());
            builder.set(REFRESH_FREQ,
                    column.getRefreshFrequency() == null ? null : column.getRefreshFrequency().getName());
            return (GenericRecord) builder.build();
        }).collectList().block();
    }

    @Override
    protected AccountMasterColumn toMetadataColumn(GenericRecord record) {
        AccountMasterColumn column = new AccountMasterColumn();
        column.setPid((Long) record.get(PID));
        column.setAmColumnId(AvroUtils.getString(record, AMCOLUMN_ID));
        column.setDataCloudVersion(AvroUtils.getString(record, DATACLOUD_VERSION));
        column.setDisplayName(AvroUtils.getString(record, DISPLAY_NAME));
        column.setDescription(AvroUtils.getString(record, DESCRIPTION));
        column.setJavaClass(AvroUtils.getString(record, JAVA_CLASS));
        column.setCategory(Category.fromName(AvroUtils.getString(record, CATEGORY)));
        column.setSubcategory(AvroUtils.getString(record, SUBCATEGORY));
        column.setStatisticalType(StatisticalType.fromName(AvroUtils.getString(record, STATISTICAL_TYPE)));
        column.setFundamentalType(FundamentalType.fromName(AvroUtils.getString(record, FUNDAMENTAL_TYPE)));
        column.setApprovedUsage(ApprovedUsage.fromName(AvroUtils.getString(record, APPROVED_USAGE)));
        column.setGroups(AvroUtils.getString(record, GROUPS));
        column.setInternalEnrichment(Boolean.TRUE.equals(record.get(IS_INTERNAL_ENRICHMENT)));
        column.setPremium(Boolean.TRUE.equals(record.get(IS_PREMIUM)));
        column.setDiscretizationStrategy(AvroUtils.getString(record, DISPLAY_DISCRETIZATION_STRATEGY));
        column.setDecodeStrategy(AvroUtils.getString(record, DECODE_STRATEGY));
        column.setEol(Boolean.TRUE.equals(record.get(IS_EOL)));
        column.setDataLicense(AvroUtils.getString(record, DATA_LICENSE));
        column.setEolVersion(AvroUtils.getString(record, EOL_VERSION));
        column.setRefreshFrequency(RefreshFrequency.fromName(AvroUtils.getString(record, REFRESH_FREQ)));
        return column;
    }

    @Override
    protected String getMDSTableName() {
        return AccountMasterColumn.TABLE_NAME;
    }
}
