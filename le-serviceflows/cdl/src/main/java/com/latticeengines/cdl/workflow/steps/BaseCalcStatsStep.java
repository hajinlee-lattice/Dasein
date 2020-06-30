package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ATTRNAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_BKTALGO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.latticeengines.common.exposed.util.AvroRecordIterator;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ColumnChanges;
import com.latticeengines.domain.exposed.spark.common.GetColumnChangesConfig;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig;
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig;
import com.latticeengines.domain.exposed.spark.stats.UpdateProfileConfig;
import com.latticeengines.serviceflows.workflow.stats.StatsProfiler;
import com.latticeengines.spark.exposed.job.common.GetColumnChangesJob;
import com.latticeengines.spark.exposed.job.stats.CalcStatsJob;
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
            }
            addStatsTableToCtx();
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
            TableRoleInCollection profileRole, String ctxKeyForRetry, List<String> includeAttrs,
            List<ProfileParameters.Attribute> declaredAttrs, boolean considerAMAttrs, boolean autoDetectCategorical,
            boolean autoDetectDiscrete) {
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

        if (!attrsToProfile.isEmpty()) {
            log.info("Going to re-profile {} attributes", attrsToProfile.size());

            // compute partial update
            List<String> attrsToUpdate = findAttrsToUpdateProfile(changes, baseData, attrsToProfile);
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
            profiler.appendResult(profileData, extraProfileUnits, new ArrayList<>(ignoreAttrs));

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
            Set<String> attrsToProfile) {
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

    @Override
    protected <V> void updateEntityValueMapInContext(String key, V value, Class<V> clz) {
        updateEntityValueMapInContext(getServingEntity(), key, value, clz);
    }

}
