package com.latticeengines.cdl.workflow.steps.report;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LatticeAccountId;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.BucketedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.camille.exposed.locks.LockManager;
import com.latticeengines.cdl.workflow.steps.process.CombineStatistics;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.report.ProfileReportStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig;
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;
import com.latticeengines.serviceflows.workflow.match.BulkMatchService;
import com.latticeengines.serviceflows.workflow.stats.StatsProfiler;
import com.latticeengines.spark.exposed.job.common.CopyJob;
import com.latticeengines.spark.exposed.job.stats.CalcStatsJob;
import com.latticeengines.spark.exposed.job.stats.ProfileJob;

/**
 * - Filter account store to (AccountId, LatticeAccountId) 2 columns
 * - Run fetch only bulk match
 * - Profile enriched data
 * - Calc stats based on enriched data and its profile
 * - Convert to stats cube
 * - Merge into existing cube
 */
@Component("generateProfileReport")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateProfileReport extends BaseSparkStep<ProfileReportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(GenerateProfileReport.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private BulkMatchService bulkMatchService;

    @Inject
    private StatsProfiler statsProfiler;

    @Resource(name = "yarnConfiguration")
    protected Configuration yarnConfiguration;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        log.info("Submitted by {}", configuration.getUserId());

        HdfsDataUnit filtered = filterAccount();
        HdfsDataUnit enriched = fetch(filtered);
        HdfsDataUnit profile = profile(enriched);
        HdfsDataUnit stats = calcStats(enriched, profile);
        upsertStats(stats);
    }

    private HdfsDataUnit filterAccount() {
        Table accountTable = dataCollectionProxy.getTable(customerSpace.toString(), ConsolidatedAccount);
        Preconditions.checkNotNull(accountTable, "There is no account data in the tenant");
        CopyConfig jobConfig = new CopyConfig();
        jobConfig.setSelectAttrs(Arrays.asList(AccountId.name(), LatticeAccountId.name()));
        DataUnit input = toDataUnit(accountTable, "Account");
        jobConfig.setInput(Collections.singletonList(input));
        SparkJobResult result = runSparkJob(CopyJob.class, jobConfig);
        return result.getTargets().get(0);
    }

    private HdfsDataUnit fetch(HdfsDataUnit inputData) {
        String avroDir = inputData.getPath();
        MatchInput matchInput = constructMatchInput(avroDir);
        MatchCommand command = bulkMatchService.match(matchInput, null);
        log.info("Bulk match finished: {}", JsonUtils.serialize(command));
        return bulkMatchService.getResultDataUnit(command, "MatchResult");
    }

    private HdfsDataUnit profile(HdfsDataUnit enrichedData) {
        String avroGlob = PathUtils.toAvroGlob(enrichedData.getPath());
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        List<ColumnMetadata> cms = schema.getFields().stream() //
                .map(field -> MetadataConverter.getAttribute(field).getColumnMetadata()).collect(Collectors.toList());
        ProfileJobConfig jobConfig = new ProfileJobConfig();
        statsProfiler.initProfileConfig(jobConfig);
        statsProfiler.classifyAttrs(cms, jobConfig);

        jobConfig.setAutoDetectCategorical(true);
        jobConfig.setAutoDetectDiscrete(true);
        jobConfig.setConsiderAMAttrs(true);

        long ts = LocalDate.now().atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli();
        jobConfig.setEvaluationDateAsTimestamp(ts);

        List<ProfileParameters.Attribute> declaredAttrs = new ArrayList<>();
        declaredAttrs.add(ProfileParameters.Attribute.nonBktAttr(AccountId.name()));
        jobConfig.setDeclaredAttrs(declaredAttrs);

        jobConfig.setInput(Collections.singletonList(enrichedData));
        SparkJobResult profileResult = runSparkJob(ProfileJob.class, jobConfig);

        HdfsDataUnit result = profileResult.getTargets().get(0);
        statsProfiler.appendResult(result);
        return result;
    }

    private HdfsDataUnit calcStats(HdfsDataUnit enrichedData, HdfsDataUnit profileData) {
        CalcStatsConfig jobConfig = new CalcStatsConfig();
        jobConfig.setInput(Arrays.asList(enrichedData, profileData));
        SparkJobResult statsResult = runSparkJob(CalcStatsJob.class, jobConfig);
        return statsResult.getTargets().get(0);
    }

    private void upsertStats(HdfsDataUnit statsData) {
        StatsCube newCube = parseStatsCube(statsData);

        DataCollection.Version active = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        String activeLock = CombineStatistics.acquireStatsLock(customerSpace.getTenantId(), active);
        DataCollection.Version inactive = active.complement();
        String inactiveLock = CombineStatistics.acquireStatsLock(customerSpace.getTenantId(), inactive);
        try {
            upsertStats(newCube, active);
        } finally {
            LockManager.releaseWriteLock(activeLock);
        }
        try {
            upsertStats(newCube, inactive);
        } finally {
            LockManager.releaseWriteLock(inactiveLock);
        }
    }

    private void upsertStats(StatsCube newCube, DataCollection.Version version) {
        StatisticsContainer container = dataCollectionProxy.getStats(customerSpace.toString(), version);
        if (container != null) {
            Map<String, StatsCube> cubes = container.getStatsCubes();
            StatsCube oldCube = cubes.get(Account.name());
            if (oldCube == null) {
                cubes.put(Account.name(), newCube);
            } else {
                oldCube.getStatistics().putAll(newCube.getStatistics());
            }
            container.setStatsCubes(cubes);
            container.setName(null);
            dataCollectionProxy.upsertStats(customerSpace.toString(), container);
        }
    }

    private StatsCube parseStatsCube(HdfsDataUnit statsData) {
        String avroGlob = PathUtils.toAvroGlob(statsData.getPath());
        AvroUtils.AvroFilesIterator itr = AvroUtils.iterateAvroFiles(yarnConfiguration, avroGlob);
        return StatsCubeUtils.parseAvro(itr);
    }

    private MatchInput constructMatchInput(String avroDir) {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setOperationalMode(OperationalMode.LDC_MATCH);

        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        matchInput.setInputBuffer(inputBuffer);

        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.LatticeAccountID, Collections.singletonList(LatticeAccountId.name()));
        matchInput.setKeyMap(keyMap);
        matchInput.setSkipKeyResolution(true);

        matchInput.setCustomSelection(selectDataCloudAttrs());

        matchInput.setFetchOnly(true);
        matchInput.setDataCloudOnly(true);
        return matchInput;
    }

    private ColumnSelection selectDataCloudAttrs() {
        // get all attributes from LDC
        List<ColumnMetadata> dcCols = columnMetadataProxy.getAllColumns("");

        // remove those in account serving store (already profiled in PA)
        String accountTableName = dataCollectionProxy.getTableName(customerSpace.toString(), BucketedAccount);
        Set<String> accCols = new HashSet<>();
        if (StringUtils.isNotBlank(accountTableName)) {
            accCols = metadataProxy.getTableColumns(customerSpace.toString(), accountTableName).stream() //
                    .map(ColumnMetadata::getAttrName).collect(Collectors.toSet());
        }

        List<Column> colsToFetch = new ArrayList<>();
        boolean useInternalAttrs = useInternalAttrs();
        for (ColumnMetadata cm : dcCols) {
            if (accCols.contains(cm.getAttrName())) {
                continue;
            }
            if (useInternalAttrs || canBeUsedInModelOrSegment(cm) || isNotInternalAttr(cm)) {
                colsToFetch.add(new Column(cm.getAttrName()));
            }
        }
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(colsToFetch);
        log.info("Added {} attributes to ColumnSelection", colsToFetch.size());
        return cs;
    }

    private boolean useInternalAttrs() {
        return configuration.isAllowInternalEnrichAttrs();
    }

    private boolean isNotInternalAttr(ColumnMetadata columnMetadata) {
        return !Boolean.TRUE.equals(columnMetadata.getCanInternalEnrich());
    }

    private boolean canBeUsedInModelOrSegment(ColumnMetadata columnMetadata) {
        return columnMetadata.isEnabledFor(ColumnSelection.Predefined.Model)
                || columnMetadata.isEnabledFor(ColumnSelection.Predefined.Segment);
    }

}
