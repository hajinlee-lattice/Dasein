package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ATTRNAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_BKTALGO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.google.common.base.Preconditions;
import com.latticeengines.camille.exposed.locks.LockManager;
import com.latticeengines.common.exposed.util.AvroRecordIterator;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ColumnChanges;
import com.latticeengines.domain.exposed.spark.common.GetColumnChangesConfig;
import com.latticeengines.domain.exposed.spark.common.UpsertConfig;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsDeltaConfig;
import com.latticeengines.domain.exposed.spark.stats.FindChangedProfileConfig;
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig;
import com.latticeengines.domain.exposed.spark.stats.UpdateProfileConfig;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.serviceflows.workflow.stats.StatsProfiler;
import com.latticeengines.spark.exposed.job.common.GetColumnChangesJob;
import com.latticeengines.spark.exposed.job.common.UpsertJob;
import com.latticeengines.spark.exposed.job.stats.CalcStatsDeltaJob;
import com.latticeengines.spark.exposed.job.stats.CalcStatsJob;
import com.latticeengines.spark.exposed.job.stats.FindChangedProfileJob;
import com.latticeengines.spark.exposed.job.stats.ProfileJob;
import com.latticeengines.spark.exposed.job.stats.UpdateProfileJob;

public abstract class BaseCalcStatsStep<T extends BaseProcessEntityStepConfiguration>
        extends BaseProcessAnalyzeSparkStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseCalcStatsStep.class);
    protected String statsTableName;
    protected boolean autoDetectCategorical; // auto detect categorical string
    protected boolean autoDetectDiscrete; // auto detect discrete number


    @Inject
    private ApplicationContext appCtx;
    private Table baseTable;

    protected Table statsTbl;
    protected Table statsDiffTbl;
    protected List<DataUnit> statsTables = new ArrayList<>();
    protected List<DataUnit> statsDiffTables = new ArrayList<>();

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

    protected String getStatsUpdatedFlagCtxKey() {
        return null;
    }

    // not using change list
    protected void executeFullCalculation() {
        if (shouldCalcStats()) {
            if (StringUtils.isBlank(statsTableName)) {
                HdfsDataUnit profileData = profileBaseTable();
                calcFullStats(profileData);
            }
            addStatsTableToCtx();
        }
    }

    protected void updateStats(boolean baseChanged, boolean enforceRebuild, TableRoleInCollection baseRole, //
                               TableRoleInCollection profileRole, String reProfileAttrsKey, String changeListKey) {
        boolean profileChanged = isChanged(profileRole);
        if (baseChanged || profileChanged || enforceRebuild) {
            Table baseTable = attemptGetTableRole(baseRole, true);
            Table profileTbl = attemptGetTableRole(profileRole, true);
            Table changeListTbl = null;
            List<String> reProfileAttrs = null;
            boolean fullReCalc = false;
            if (enforceRebuild) {
                log.info("Need to fully re-calculate {} stats, due to enforced rebuild", baseRole);
                fullReCalc = true;
            } else {
                changeListTbl = getTableSummaryFromKey(customerSpaceStr, changeListKey);
                if (changeListTbl == null) {
                    log.info("Need to fully re-calculate {} stats, because there is no change list table.", baseRole);
                    fullReCalc = true;
                } else {
                    if (profileChanged) {
                        reProfileAttrs = getListObjectFromContext(reProfileAttrsKey, String.class);
                        if (reProfileAttrs == null) {
                            log.info("Need to fully re-calculate {} stats, because there is no list in {}", //
                                    baseRole, reProfileAttrsKey);
                            fullReCalc = true;
                        }
                    }
                }
            }
            if (fullReCalc) {
                HdfsDataUnit statsResult = calcStats(baseTable, profileTbl.toHdfsDataUnit("Profile"));
                statsResult.setName(baseRole + "Stats");
                statsTables.add(statsResult);
            } else {
                // partial re-calculate
                Preconditions.checkNotNull(changeListTbl, "Must have change list table " + changeListKey);
                HdfsDataUnit profileData = profileTbl.toHdfsDataUnit("Profile");
                List<String> partialAttrs = new ArrayList<>(Arrays.asList(baseTable.getAttributeNames()));
                if (CollectionUtils.isNotEmpty(reProfileAttrs)) {
                    // handle re-profile attrs
                    log.info("There are {} attributes need full stats calculation.", reProfileAttrs.size());
                    HdfsDataUnit statsResult = calcStats(baseTable, profileData, reProfileAttrs);
                    statsResult.setName(baseRole + "Stats");
                    statsTables.add(statsResult);

                    partialAttrs.removeAll(reProfileAttrs);
                } else {
                    log.info("There are no attributes need full stats calculation.");
                }
                if (CollectionUtils.isNotEmpty(partialAttrs)) {
                    log.info("There are {} attributes need partial stats calculation.", partialAttrs.size());
                    CalcStatsDeltaConfig deltaConfig = new CalcStatsDeltaConfig();
                    deltaConfig.setIncludeAttrs(partialAttrs);
                    deltaConfig.setInput(Arrays.asList( //
                            changeListTbl.toHdfsDataUnit("ChangeList"), profileData) //
                    );
                    SparkJobResult sparkJobResult = runSparkJob(CalcStatsDeltaJob.class, deltaConfig);
                    statsDiffTables.add(sparkJobResult.getTargets().get(0));
                } else {
                    log.info("There are no attributes need partial stats calculation.");
                }

            }
        } else {
            log.info("No reason to re-calculate {} stats.", baseRole);
        }
    }

    protected void mergeStats() {
        HdfsDataUnit statsData = null;
        if (statsTables.size() > 1) {
            UpsertConfig upsertConfig = new UpsertConfig();
            upsertConfig.setJoinKey(DataCloudConstants.PROFILE_ATTR_ATTRNAME);
            upsertConfig.setInput(statsTables);
            SparkJobResult result = runSparkJob(UpsertJob.class, upsertConfig);
            statsData = result.getTargets().get(0);
        } else if (statsTables.size() == 1) {
            statsData = (HdfsDataUnit) statsTables.get(0);
        }
        if (statsData != null) {
            statsTableName = NamingUtils.timestamp("AccountStats");
            statsTbl = toTable(statsTableName, PROFILE_ATTR_ATTRNAME, statsData);
            metadataProxy.createTable(customerSpaceStr, statsTableName, statsTbl);
        }
    }

    protected void mergeStatsDiff() {
        HdfsDataUnit statsData = null;
        if (statsDiffTables.size() > 1) {
            UpsertConfig upsertConfig = new UpsertConfig();
            upsertConfig.setJoinKey(DataCloudConstants.PROFILE_ATTR_ATTRNAME);
            upsertConfig.setInput(statsDiffTables);
            SparkJobResult result = runSparkJob(UpsertJob.class, upsertConfig);
            statsData = result.getTargets().get(0);
        } else if (statsDiffTables.size() == 1) {
            statsData = (HdfsDataUnit) statsDiffTables.get(0);
        }
        if (statsData != null) {
            String statsDiffTableName = NamingUtils.timestamp("AccountStatsDiff");
            statsDiffTbl = toTable(statsDiffTableName, PROFILE_ATTR_ATTRNAME, statsData);
            metadataProxy.createTable(customerSpaceStr, statsDiffTableName, statsDiffTbl);
        }
    }

    protected void prepare() {
        bootstrap();
        if (StringUtils.isNotBlank(getStatsTableCtxKey())) {
            Table tableInCtx = getTableSummaryFromKey(customerSpaceStr, getStatsTableCtxKey());
            if (tableInCtx != null) {
                statsTableName = tableInCtx.getName();
            }
        }
    }

    protected HdfsDataUnit profileBaseTable() {
        if (StringUtils.isNotBlank(getProfileTableCtxKey())) {
            Table tableInCtx = getTableSummaryFromKey(customerSpaceStr, getProfileTableCtxKey());
            if (tableInCtx != null) {
                log.info("Found profile table in context, skip profiling.");
                return tableInCtx.toHdfsDataUnit(getProfileRole().name());
            }
        }
        Table baseTable = getBaseTable();
        List<ProfileParameters.Attribute> declaredAttrs = getDeclaredAttrs();
        return profile(baseTable, getProfileRole(), getProfileTableCtxKey(), null, //
                declaredAttrs, false, autoDetectCategorical, autoDetectDiscrete);
    }

    protected HdfsDataUnit profile(Table baseTable, TableRoleInCollection profileRole, String ctxKeyForRetry,
            List<String> includeAttrs, List<ProfileParameters.Attribute> declaredAttrs, boolean considerAMAttrs,
            boolean autoDetectCategorical, boolean autoDetectDiscrete) {
        HdfsDataUnit inputData = baseTable.toHdfsDataUnit("BaseTable");
        ProfileJobConfig jobConfig = new ProfileJobConfig();
        jobConfig.setInput(Collections.singletonList(inputData));

        StatsProfiler profiler = initializeProfiler(baseTable, jobConfig, includeAttrs, //
                declaredAttrs, considerAMAttrs, autoDetectCategorical, autoDetectDiscrete);

        SparkJobResult profileResult = runSparkJob(ProfileJob.class, jobConfig);
        HdfsDataUnit profileData = profileResult.getTargets().get(0);
        profiler.appendResult(profileData);

        if (profileRole != null) {
            profileData = saveProfileData(profileRole, profileData, ctxKeyForRetry);
        }

        return profileData;
    }

    protected HdfsDataUnit profileWithChangeList(Table baseTable, Table changeListTbl, Table oldProfileTbl,
            TableRoleInCollection profileRole, String ctxKeyForRetry, String ctxKeyForReProfileAttrs, List<String> includeAttrs,
            List<ProfileParameters.Attribute> declaredAttrs, boolean considerAMAttrs, boolean ignoreDateAttrs,
                                                 boolean autoDetectCategorical, boolean autoDetectDiscrete) {
        ProfileJobConfig profileConfig = new ProfileJobConfig();
        initializeProfiler(baseTable, profileConfig, includeAttrs, //
                declaredAttrs, considerAMAttrs, autoDetectCategorical, autoDetectDiscrete);
        Set<String> catAttrsBasedNewData = profileConfig.getCatAttrs().stream() //
                .map(ProfileParameters.Attribute::getAttr).collect(Collectors.toSet());
        Set<String> numAttrsBasedNewData = profileConfig.getNumericAttrs().stream() //
                .map(ProfileParameters.Attribute::getAttr).collect(Collectors.toSet());

        HdfsDataUnit baseData = baseTable.toHdfsDataUnit("BaseData");
        HdfsDataUnit changeListData = changeListTbl.toHdfsDataUnit("ChangeList");
        HdfsDataUnit oldProfileData = oldProfileTbl.toHdfsDataUnit("PreviousProfile");

        // exclude free text and interval attributes
        restrictByPreviousProfile(oldProfileData, baseTable, catAttrsBasedNewData, numAttrsBasedNewData);

        // only need to check if cat or num attrs have change
        Set<String> attrsToProfile = new HashSet<>(catAttrsBasedNewData);
        attrsToProfile.addAll(numAttrsBasedNewData);
        ColumnChanges changes = getColumnChanges(changeListData, attrsToProfile);
        attrsToProfile.retainAll(changes.getChanged().keySet());
        if (!ignoreDateAttrs) {
            // need to include all date attributes
            baseTable.getColumnMetadata().forEach(cm -> {
                if (LogicalDataType.Date.equals(cm.getLogicalDataType()) && //
                        !evaluationDateStr.equals(cm.getLastDataRefresh())) {
                    attrsToProfile.add(cm.getAttrName());
                }
            });
        }
        if (CollectionUtils.isNotEmpty(declaredAttrs)) {
            declaredAttrs.forEach(da -> attrsToProfile.remove(da.getAttr()));
        }

        if (!attrsToProfile.isEmpty()) {
            log.info("Going to re-profile {} attributes", attrsToProfile.size());

            // compute partial update
            List<String> attrsToUpdate = findAttrsToUpdateProfile(changes, baseData, attrsToProfile, declaredAttrs);
            HdfsDataUnit updatedProfile = null;
            if (!attrsToUpdate.isEmpty()) {
                log.info("Going to try partial update {} attributes's profile", attrsToUpdate.size());
                updatedProfile = updateProfile(changeListData, oldProfileData, attrsToUpdate);
                List<String> attrsUpdated = getAttrsUpdated(updatedProfile);
                log.info("Partially updated {} attributes's profile", attrsUpdated.size());
                attrsToProfile.removeAll(attrsUpdated);
            }

            profileConfig = new ProfileJobConfig();
            StatsProfiler profiler = initializeProfiler(baseTable, profileConfig, new ArrayList<>(attrsToProfile), //
                    declaredAttrs, considerAMAttrs, autoDetectCategorical, autoDetectDiscrete);
            profileConfig.setInput(Collections.singletonList(baseTable.toHdfsDataUnit("BaseTable")));
            SparkJobResult reProfileResult = runSparkJob(ProfileJob.class, profileConfig);
            HdfsDataUnit profileData = reProfileResult.getTargets().get(0);
            Set<String> ignoreAttrs = new HashSet<>(changes.getRemoved());
            ignoreAttrs.addAll(attrsToProfile);
            List<HdfsDataUnit> extraProfileUnits = new ArrayList<>();
            extraProfileUnits.add(oldProfileData);
            if (updatedProfile != null) {
                extraProfileUnits.add(updatedProfile);
            }
            profiler.appendResult(profileData, updatedProfile, oldProfileData, new ArrayList<>(ignoreAttrs));

            verifyNoDuplicatedProfile(profileData);

            FindChangedProfileConfig findChangeConfig = new FindChangedProfileConfig();
            findChangeConfig.setInput(Arrays.asList(oldProfileData, profileData));
            SparkJobResult changeSparkResult = runSparkJob(FindChangedProfileJob.class, findChangeConfig);
            List<?> lst = JsonUtils.deserialize(changeSparkResult.getOutput(), List.class);
            List<String> attrs = JsonUtils.convertList(lst, String.class);
            if (StringUtils.isNotBlank(ctxKeyForReProfileAttrs)) {
                log.info("There are {} attributes having changed profile.", attrs.size());
                putObjectInContext(ctxKeyForReProfileAttrs, attrs);
            }

            if (profileRole != null) {
                profileData = saveProfileData(profileRole, profileData, ctxKeyForRetry);
            }

            return profileData;
        } else {
            log.info("No attributes need re-profile.");
            linkInactiveTable(profileRole);
            return attemptGetTableRole(profileRole, true).toHdfsDataUnit("Profile");
        }
    }

    private void verifyNoDuplicatedProfile(HdfsDataUnit profileData) {
        String avroGlob = PathUtils.toAvroGlob(profileData.getPath());
        AvroRecordIterator itr = AvroUtils.iterateAvroFiles(yarnConfiguration, avroGlob);
        Set<String> seenAttrs = new HashSet<>();
        List<String> duplicatedAttrs = new ArrayList<>();
        while(itr.hasNext()) {
            GenericRecord record = itr.next();
            String attrName = record.get(PROFILE_ATTR_ATTRNAME).toString();
            if (!seenAttrs.contains(attrName)) {
                seenAttrs.add(attrName);
            } else {
                duplicatedAttrs.add(attrName);
            }
        }
        if (CollectionUtils.isNotEmpty(duplicatedAttrs)) {
            throw new IllegalArgumentException("Duplicated attributs in profile: " //
                    + StringUtils.join(duplicatedAttrs, ","));
        }
    }

    private void restrictByPreviousProfile(HdfsDataUnit oldProfile, Table baseTable, Collection<String> catAttrs,
            Collection<String> numAttrs) {
        String avroGlob = PathUtils.toAvroGlob(oldProfile.getPath());
        AvroRecordIterator itr = AvroUtils.iterateAvroFiles(yarnConfiguration, avroGlob);
        Set<String> existingCatAttrs = new HashSet<>();
        Set<String> existingDisAttrs = new HashSet<>();
        Set<String> newAttrs = new HashSet<>(Arrays.asList(baseTable.getAttributeNames()));
        while (itr.hasNext()) {
            GenericRecord record = itr.next();
            String attrName = record.get(PROFILE_ATTR_ATTRNAME).toString();
            newAttrs.remove(attrName);
            Object val = record.get(PROFILE_ATTR_BKTALGO);
            if (val != null) {
                BucketAlgorithm algo = JsonUtils.deserialize(val.toString(), BucketAlgorithm.class);
                if (algo instanceof CategoricalBucket) {
                    existingCatAttrs.add(attrName);
                }
                if (algo instanceof DiscreteBucket) {
                    existingDisAttrs.add(attrName);
                }
            }
        }
        existingCatAttrs.addAll(newAttrs);
        existingDisAttrs.addAll(newAttrs);
        catAttrs.retainAll(existingCatAttrs);
        numAttrs.retainAll(existingDisAttrs);
    }

    private ColumnChanges getColumnChanges(HdfsDataUnit changeList, Collection<String> attrsToProfile) {
        GetColumnChangesConfig jobConfig = new GetColumnChangesConfig();
        jobConfig.setInput(Collections.singletonList(changeList));
        jobConfig.setIncludeAttrs(new ArrayList<>(attrsToProfile));
        SparkJobResult sparkJobResult = runSparkJob(GetColumnChangesJob.class, jobConfig);
        return JsonUtils.deserialize(sparkJobResult.getOutput(), ColumnChanges.class);
    }

    private List<String> findAttrsToUpdateProfile(ColumnChanges changes, HdfsDataUnit baseTable,
            Set<String> attrsToProfile, List<ProfileParameters.Attribute> declareAttrs) {
        List<String> attrsToUpdate = new ArrayList<>();
        long totalCnt = baseTable.getCount();
        if (totalCnt > 10000) {
            // if total cnt < 10K, re-profile all
            Map<String, Long> changeCntMap = changes.getChanged();
            for (String attr : attrsToProfile) {
                long changeCnt = changeCntMap.getOrDefault(attr, totalCnt);
                double changeRatio = 1.D * changeCnt / totalCnt;
                if (changeRatio <= 0.3) {
                    log.info("Attribute {} has less than 30% change ({}), going to partially update its profile", //
                            attr, changeRatio);
                    attrsToUpdate.add(attr);
                }
            }
        }
        if (CollectionUtils.isNotEmpty(declareAttrs)) {
            declareAttrs.forEach(da -> attrsToUpdate.remove(da.getAttr()));
        }
        return attrsToUpdate;
    }

    private HdfsDataUnit updateProfile(HdfsDataUnit changeList, HdfsDataUnit oldProfile,
            Collection<String> attrsToUpdate) {
        UpdateProfileConfig updateConfig = new UpdateProfileConfig();
        updateConfig.setIncludeAttrs(new ArrayList<>(attrsToUpdate));
        updateConfig.setInput(Arrays.asList(changeList, oldProfile));
        SparkJobResult updateSparkResult = runSparkJob(UpdateProfileJob.class, updateConfig);
        return updateSparkResult.getTargets().get(0);
    }

    private List<String> getAttrsUpdated(HdfsDataUnit updateProfile) {
        String avroGlob = PathUtils.toAvroGlob(updateProfile.getPath());
        AvroRecordIterator itr = AvroUtils.iterateAvroFiles(yarnConfiguration, avroGlob);
        List<String> attrsUpdated = new ArrayList<>();
        while (itr.hasNext()) {
            GenericRecord record = itr.next();
            String attrName = record.get(PROFILE_ATTR_ATTRNAME).toString();
            attrsUpdated.add(attrName);
        }
        return attrsUpdated;
    }

    private HdfsDataUnit saveProfileData(TableRoleInCollection profileRole, HdfsDataUnit profileData,
            String ctxKeyForRetry) {
        // save profile table
        String tenantId = CustomerSpace.shortenCustomerSpace(customerSpaceStr);
        String profileTableName = NamingUtils.timestamp(profileRole.name());
        Table profileTable = toTable(profileTableName, PROFILE_ATTR_ATTRNAME, profileData);
        profileData = profileTable.toHdfsDataUnit("Profile");
        metadataProxy.createTable(tenantId, profileTableName, profileTable);
        dataCollectionProxy.upsertTable(customerSpaceStr, profileTableName, profileRole, inactive);
        if (StringUtils.isNotBlank(ctxKeyForRetry)) {
            exportToS3AndAddToContext(profileTable, ctxKeyForRetry);
        }
        return profileData;
    }

    private StatsProfiler initializeProfiler(Table baseTbl, ProfileJobConfig profileConfig, List<String> includeAttrs,
            List<ProfileParameters.Attribute> declaredAttrs, boolean considerAMAttrs, boolean autoDetectCategorical,
            boolean autoDetectDiscrete) {
        StatsProfiler profiler = appCtx.getBean(StatsProfiler.class);
        List<ColumnMetadata> cms = baseTbl.getColumnMetadata();
        profiler.initProfileConfig(profileConfig);
        if (CollectionUtils.isNotEmpty(declaredAttrs)) {
            profileConfig.setDeclaredAttrs(declaredAttrs);
        }
        profileConfig.setAutoDetectCategorical(autoDetectCategorical);
        profileConfig.setAutoDetectDiscrete(autoDetectDiscrete);
        setEvaluationDateStrAndTimestamp();
        profileConfig.setEvaluationDateAsTimestamp(evaluationDateAsTimestamp);
        profileConfig.setConsiderAMAttrs(considerAMAttrs);
        profileConfig.setIncludeAttrs(includeAttrs);
        profiler.classifyAttrs(cms, profileConfig);
        return profiler;
    }

    protected void calcFullStats(HdfsDataUnit profileData) {
        if (StringUtils.isNotBlank(statsTableName)) {
            log.info("Found stats table in context, skip caculating stats.");
        } else {
            HdfsDataUnit statsResult = calcStats(getBaseTable(), profileData);
            if (getProfileRole() == null) {
                // not saving profile data
                clearTempData(profileData);
            }
            String tenantId = CustomerSpace.shortenCustomerSpace(customerSpaceStr);
            statsTableName = NamingUtils.timestamp(getServingEntity().name() + "Stats");
            Table statsTable = toTable(statsTableName, PROFILE_ATTR_ATTRNAME, statsResult);
            metadataProxy.createTable(tenantId, statsTableName, statsTable);
            if (StringUtils.isNotBlank(getStatsTableCtxKey())) {
                exportToS3AndAddToContext(statsTable, getStatsTableCtxKey());
            }
        }
    }

    protected HdfsDataUnit calcStats(Table baseTable, HdfsDataUnit profileData) {
        HdfsDataUnit inputData = baseTable.toHdfsDataUnit("BaseTable");
        CalcStatsConfig jobConfig = new CalcStatsConfig();
        jobConfig.setInput(Arrays.asList(inputData, profileData));
        SparkJobResult statsResult = runSparkJob(CalcStatsJob.class, jobConfig);
        return statsResult.getTargets().get(0);
    }

    protected HdfsDataUnit calcStats(Table baseTable, HdfsDataUnit profileData, List<String> includeAttrs) {
        HdfsDataUnit inputData = baseTable.toHdfsDataUnit("BaseTable");
        CalcStatsConfig jobConfig = new CalcStatsConfig();
        jobConfig.setInput(Arrays.asList(inputData, profileData));
        jobConfig.setIncludeAttrs(includeAttrs);
        SparkJobResult statsResult = runSparkJob(CalcStatsJob.class, jobConfig);
        return statsResult.getTargets().get(0);
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
        return super.isToReset(getServingEntity());
    }

    protected Table getBaseTable() {
        if (baseTable == null) {
            TableRoleInCollection servingRole = getServingEntity().getServingStore();
            baseTable = attemptGetTableRole(servingRole, true);
        }
        return baseTable;
    }

    protected long getBaseTableCount() {
        Table baseTbl = attemptGetTableRole(getBaseTableRole(), true);
        return baseTbl.toHdfsDataUnit("Base").getCount();
    }

    protected void cleanupStatsCube() {
        String lockName = acquireStatsLock(CustomerSpace.shortenCustomerSpace(customerSpaceStr), inactive);
        try {
            String cubeName = configuration.getMainEntity().name();
            Map<String, StatsCube> cubeMap = getCurrentCubeMap();
            cubeMap.remove(cubeName);
            saveStatsContainer(cubeMap);
        } finally {
            LockManager.releaseWriteLock(lockName);
        }
    }

    protected void upsertStatsCube() {
        boolean alreadyUpdated = Boolean.TRUE.equals(getObjectFromContext(getStatsUpdatedFlagCtxKey(), Boolean.class));
        if (alreadyUpdated) {
            log.info("Stats already updated according to ctx ACCOUNT_STATS_UPDATED.");
            return;
        }

        long tblCnt = getBaseTableCount();
        StatsCube replaceCube = null;
        if (statsTbl != null) {
            replaceCube = getStatsCube(statsTbl.toHdfsDataUnit("Stats"));
            log.info("Parsed a replace cube of {} attributes", replaceCube.getStatistics().size());
        }

        StatsCube updateCube = null;
        if (statsDiffTbl != null) {
            updateCube = getStatsCube(statsDiffTbl.toHdfsDataUnit("StatsDiff"));
            log.info("Parsed a update cube of {} attributes", updateCube.getStatistics().size());
        }

        String lockName = acquireStatsLock(CustomerSpace.shortenCustomerSpace(customerSpaceStr), inactive);
        try {
            String cubeName = configuration.getMainEntity().name();
            Map<String, StatsCube> cubeMap = getCurrentCubeMap();
            Map<String, AttributeStats> attrStats = new HashMap<>();
            if (cubeMap.containsKey(cubeName)) {
                StatsCube oldCube = cubeMap.get(cubeName);
                attrStats.putAll(oldCube.getStatistics());
            }
            if (updateCube != null && MapUtils.isNotEmpty(updateCube.getStatistics())) {
                for (String attr: updateCube.getStatistics().keySet()) {
                    AttributeStats updateStat = updateCube.getStatistics().get(attr);
                    if (attrStats.containsKey(attr)) {
                        attrStats.put(attr, mergeAttrStat(attrStats.get(attr), updateStat));
                    } else {
                        attrStats.put(attr, updateStat);
                    }
                }
            }
            if (replaceCube != null && MapUtils.isNotEmpty(replaceCube.getStatistics())) {
                attrStats.putAll(replaceCube.getStatistics());
            }
            StatsCube cube = new StatsCube();
            cube.setCount(tblCnt);
            cube.setStatistics(attrStats);
            cubeMap.put(cubeName, cube);
            saveStatsContainer(cubeMap);
            putObjectInContext(getStatsUpdatedFlagCtxKey(), Boolean.TRUE);
        } finally {
            LockManager.releaseWriteLock(lockName);
        }
    }

    private AttributeStats mergeAttrStat(AttributeStats baseStat, AttributeStats statDiff) {
        if (statDiff.getBuckets() != null && CollectionUtils.isNotEmpty(statDiff.getBuckets().getBucketList())) {
            Buckets buckets = baseStat.getBuckets();
            Map<Long, Bucket> bucketMap = new HashMap<>();
            if (CollectionUtils.isNotEmpty(buckets.getBucketList())) {
                bucketMap.putAll(buckets.getBucketList().stream()
                        .collect(Collectors.toMap(Bucket::getId, Function.identity())));
            }
            statDiff.getBuckets().getBucketList().forEach(bktDiff -> {
                long bktId = bktDiff.getId();
                if (bucketMap.containsKey(bktId)) {
                    Bucket baseBkt = bucketMap.get(bktId);
                    baseBkt.setCount(baseBkt.getCount() + bktDiff.getCount());
                } else {
                    bucketMap.put(bktId, bktDiff);
                }
            });
            List<Bucket> bucketList = bucketMap.values().stream() //
                    .sorted(Comparator.comparing(Bucket::getId)) //
                    .collect(Collectors.toList());
            buckets.setBucketList(bucketList);
        }
        baseStat.setNonNullCount(baseStat.getNonNullCount() + statDiff.getNonNullCount());
        return baseStat;
    }

    // directly save active version stats to inactive version
    protected void linkStatsContainer() {
        Map<String, StatsCube> cubeMap = getCurrentCubeMap();
        if (MapUtils.isNotEmpty(cubeMap)) {
            saveStatsContainer(cubeMap);
        } else {
            log.info("Skip saving an empty stats.");
        }
    }

    private void saveStatsContainer(Map<String, StatsCube> cubeMap) {
        StatisticsContainer statsContainer = new StatisticsContainer();
        String statsName = NamingUtils.timestamp("Stats");
        statsContainer.setName(statsName);
        statsContainer.setStatsCubes(cubeMap);
        statsContainer.setVersion(inactive);
        log.info("Saving stats " + statsName + " with " + cubeMap.size() + " cubes.");
        dataCollectionProxy.upsertStats(customerSpaceStr, statsContainer);
        putObjectInContext(STATS_UPDATED, Boolean.TRUE); // flag to indicate any stat is updated
    }

    protected Map<String, StatsCube> getCurrentCubeMap() {
        Map<String, StatsCube> cubeMap = new HashMap<>();
        StatisticsContainer latestStatsContainer = dataCollectionProxy.getStats(customerSpaceStr, inactive);
        DataCollection.Version latestStatsVersion = null;
        if (latestStatsContainer == null) {
            latestStatsContainer = dataCollectionProxy.getStats(customerSpaceStr, active);
            if (latestStatsContainer != null) {
                latestStatsVersion = active;
            }
        } else {
            latestStatsVersion = inactive;
        }
        if (latestStatsContainer != null && MapUtils.isNotEmpty(latestStatsContainer.getStatsCubes())) {
            cubeMap.putAll(latestStatsContainer.getStatsCubes());
        }
        String msg = "Found {} cubes in the stats in {}";
        if (MapUtils.isNotEmpty(cubeMap)) {
            msg += " : " + StringUtils.join(cubeMap.keySet(), ", ");
        }
        log.info(msg, MapUtils.size(cubeMap), latestStatsVersion);
        return cubeMap;
    }

    protected StatsCube getStatsCube(HdfsDataUnit statsData) {
        String avroGlob = PathUtils.toAvroGlob(statsData.getPath());
        log.info("Checking for stats file at {}", avroGlob);
        try (AvroUtils.AvroFilesIterator records = AvroUtils.iterateAvroFiles(yarnConfiguration, avroGlob)) {
            return StatsCubeUtils.parseAvro(records);
        }
    }

    public static String acquireStatsLock(String tenantId, DataCollection.Version version) {
        String lockName = "AtlastStatsLock_" + tenantId + "_" + version;
        try {
            LockManager.registerCrossDivisionLock(lockName);
            LockManager.acquireWriteLock(lockName, 1, TimeUnit.HOURS);
            log.info("Won the distributed lock {}", lockName);
        } catch (Exception e) {
            log.warn("Error while acquiring zk lock {}", lockName, e);
        }
        return lockName;
    }

    @Override
    protected <V> void updateEntityValueMapInContext(String key, V value, Class<V> clz) {
        updateEntityValueMapInContext(getServingEntity(), key, value, clz);
    }
}
