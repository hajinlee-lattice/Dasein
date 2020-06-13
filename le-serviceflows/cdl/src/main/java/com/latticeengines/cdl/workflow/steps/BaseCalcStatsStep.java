package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ATTRNAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig;
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig;
import com.latticeengines.serviceflows.workflow.stats.StatsProfiler;
import com.latticeengines.spark.exposed.job.stats.CalcStatsJob;
import com.latticeengines.spark.exposed.job.stats.ProfileJob;

public abstract class BaseCalcStatsStep<T extends BaseProcessEntityStepConfiguration> extends BaseProcessAnalyzeSparkStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseCalcStatsStep.class);

    @Inject
    private StatsProfiler statsProfiler;

    protected String statsTableName;
    private Table baseTable;

    protected boolean autoDetectCategorical; // auto detect categorical string
    protected boolean autoDetectDiscrete; // auto detect discrete number

    // check if the entity is to be removed (reset)
    protected BusinessEntity getServingEntity() {
        return configuration.getMainEntity();
    }

    // based on which the stats is to be calculated
    protected TableRoleInCollection getBaseTableRole() {
        return getServingEntity().getServingStore();
    }

    protected abstract String getStatsTableCtxKey();

    // override this if need to persist the profile table
    // profile table needs to be persisted if using change list
    protected TableRoleInCollection getProfileRole() {
        return null;
    }
    protected String getProfileTableCtxKey() {
        return null;
    }

    // not using change list
    protected void executeFullCalculation() {
        if (shouldCalcStats()) {
            if (StringUtils.isBlank(statsTableName)) {
                HdfsDataUnit profileData = profileBaseTable();
                calcFullStats(profileData);
                clearAllWorkspacesAsync();
            }
            addStatsTableToCtx();
        }
    }

    protected void prepare() {
        bootstrap();
        if (StringUtils.isNotBlank(getStatsTableCtxKey())) {
            Table tableInCtx = getTableSummaryFromKey(customerSpace.toString(), getStatsTableCtxKey());
            if (tableInCtx != null) {
                statsTableName = tableInCtx.getName();
            }
        }
    }

    protected HdfsDataUnit profileBaseTable() {
        if (StringUtils.isNotBlank(getProfileTableCtxKey())) {
            Table tableInCtx = getTableSummaryFromKey(customerSpace.toString(), getProfileTableCtxKey());
            if (tableInCtx != null) {
                log.info("Found profile table in context, skip profiling.");
                return tableInCtx.toHdfsDataUnit(getProfileRole().name());
            }
        }

        Table baseTable = getBaseTable();
        List<ColumnMetadata> cms = baseTable.getColumnMetadata();
        ProfileJobConfig jobConfig = new ProfileJobConfig();
        statsProfiler.initProfileConfig(jobConfig);
        statsProfiler.classifyAttrs(cms, jobConfig);

        jobConfig.setAutoDetectCategorical(autoDetectCategorical);
        jobConfig.setAutoDetectDiscrete(autoDetectDiscrete);
        setEvaluationDateStrAndTimestamp();
        jobConfig.setEvaluationDateAsTimestamp(evaluationDateAsTimestamp);
        jobConfig.setConsiderAMAttrs(false);
        List<ProfileParameters.Attribute> declaredAttrs = getDeclaredAttrs();
        if (CollectionUtils.isNotEmpty(declaredAttrs)) {
            jobConfig.setDeclaredAttrs(declaredAttrs);
        }

        HdfsDataUnit inputData = baseTable.toHdfsDataUnit(getBaseTableRole().name());
        jobConfig.setInput(Collections.singletonList(inputData));
        SparkJobResult profileResult = runSparkJob(ProfileJob.class, jobConfig);
        HdfsDataUnit profileData =  profileResult.getTargets().get(0);

        if (getProfileRole() != null) {
            // save profile table
            String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
            String profileTableName = NamingUtils.timestamp(getProfileRole().name());
            Table profileTable = toTable(profileTableName, PROFILE_ATTR_ATTRNAME, profileData);
            profileData = profileTable.toHdfsDataUnit("Profile");
            metadataProxy.createTable(tenantId, profileTableName, profileTable);
            exportToS3AndAddToContext(profileTable, getProfileTableCtxKey());
        }

        return profileData;
    }

    protected void calcFullStats(HdfsDataUnit profileData) {
        if (StringUtils.isNotBlank(statsTableName)) {
            log.info("Found stats table in context, skip caculating stats.");
        } else {
            Table baseTable = getBaseTable();
            HdfsDataUnit inputData = baseTable.toHdfsDataUnit(getBaseTableRole().name());

            CalcStatsConfig jobConfig = new CalcStatsConfig();
            jobConfig.setInput(Arrays.asList(inputData, profileData));

            SparkJobResult statsResult = runSparkJob(CalcStatsJob.class, jobConfig);
            String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
            statsTableName = NamingUtils.timestamp(getServingEntity().name() + "Stats");
            Table statsTable = toTable(statsTableName, PROFILE_ATTR_ATTRNAME, statsResult.getTargets().get(0));
            metadataProxy.createTable(tenantId, statsTableName, statsTable);
            if (StringUtils.isNotBlank(getStatsTableCtxKey())) {
                exportToS3AndAddToContext(statsTable, getStatsTableCtxKey());
            }
        }
    }

    // attrs with declared profile strategy
    protected List<ProfileParameters.Attribute> getDeclaredAttrs() {
        List<ProfileParameters.Attribute> pAttrs = new ArrayList<>();
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(getBaseTableRole().getPrimaryKey()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(InterfaceName.CDLCreatedTime.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(InterfaceName.CDLUpdatedTime.name()));
        return pAttrs;
    }

    protected void addStatsTableToCtx() {
        updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);
    }

    private boolean shouldCalcStats() {
        boolean shouldCalcStats = false;
        if (isToReset()) {
            log.info("No need to calc stats for {}, as it is to be reset.", getServingEntity());
            shouldCalcStats = false;
        } else if (isChanged(getBaseTableRole())) {
            log.info("Should calc stats for {}, as the base table {} has changed.", //
                    getServingEntity(), getBaseTableRole());
            shouldCalcStats = true;
        }
        return shouldCalcStats;
    }

    // reset means remove this entity
    private boolean isToReset() {
        Set<BusinessEntity> resetEntities = getSetObjectFromContext(RESET_ENTITIES, BusinessEntity.class);
        return CollectionUtils.isNotEmpty(resetEntities) && resetEntities.contains(getServingEntity());
    }

    protected Table getBaseTable() {
        if (baseTable == null) {
            TableRoleInCollection servingRole = getServingEntity().getServingStore();
            baseTable = attemptGetTableRole(servingRole, true);
        }
        return baseTable;
    }

    @Override
    protected <V> void updateEntityValueMapInContext(String key, V value, Class<V> clz) {
        updateEntityValueMapInContext(getServingEntity(), key, value, clz);
    }

}
