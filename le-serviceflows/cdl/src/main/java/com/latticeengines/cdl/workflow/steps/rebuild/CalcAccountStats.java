package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ATTRNAME;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountProfile;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccountProfile;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.camille.exposed.locks.LockManager;
import com.latticeengines.cdl.workflow.steps.BaseCalcStatsStep;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.UpsertConfig;
import com.latticeengines.spark.exposed.job.common.UpsertJob;

@Lazy
@Component("calcAccountStats")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalcAccountStats extends BaseCalcStatsStep<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CalcAccountStats.class);

    @Override
    protected TableRoleInCollection getProfileRole() {
        return null;
    }

    @Override
    protected String getProfileTableCtxKey() {
        return null;
    }

    @Override
    protected String getStatsTableCtxKey() {
        return ACCOUNT_STATS_TABLE_NAME;
    }

    private boolean enforceRebuild;
    private boolean customerAccountChanged;
    private boolean latticeAccountChanged;
    private List<DataUnit> statsTables = new ArrayList<>();
    private Table statsTbl;
    private Table statsDiffTbl;

    @Override
    public void execute() {
        prepare();
        enforceRebuild = Boolean.TRUE.equals(configuration.getRebuild());
        if (shouldDoNothing()) {
            log.info("No need to update Account stats.");
            linkStatsContainer();
        } else {
            statsTbl = getTableSummaryFromKey(customerSpaceStr, getStatsTableCtxKey());
            statsDiffTbl = getTableSummaryFromKey(customerSpaceStr, ACCOUNT_STATS_DIFF_TABLE_NAME);
            if (statsTbl == null && statsDiffTbl == null) {
                updateCustomerStats();
                updateLatticeStats();
                mergeStats();

                // for retry
                exportToS3AndAddToContext(statsTbl, getStatsTableCtxKey());
            }
            upsertStatsCube();
        }
    }

    private void updateCustomerStats() {
        updateStats(customerAccountChanged, enforceRebuild, ConsolidatedAccount, AccountProfile, //
                ACCOUNT_RE_PROFILE_ATTRS, ACCOUNT_CHANGELIST_TABLE_NAME);
    }

    private void updateLatticeStats() {
        boolean rebuildLatticeAccount = enforceRebuild && //
                Boolean.TRUE.equals(getObjectFromContext(REBUILD_LATTICE_ACCOUNT, Boolean.class));
        updateStats(latticeAccountChanged, rebuildLatticeAccount, LatticeAccount, LatticeAccountProfile, //
                LATTICE_ACCOUNT_RE_PROFILE_ATTRS, LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME);
    }

    private void updateStats(boolean baseChanged, boolean enforceRebuild, TableRoleInCollection baseRole,
                             TableRoleInCollection profileRole, String reProfileAttrsKey, String changeListKey) {
        if (baseChanged || enforceRebuild) {
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
                    reProfileAttrs = getListObjectFromContext(reProfileAttrsKey, String.class);
                    if (reProfileAttrs == null) {
                        log.info("Need to fully re-calculate {} stats, because there is no list in {}", //
                                baseRole, reProfileAttrsKey);
                        fullReCalc = true;
                    }
                }
            }
            if (fullReCalc) {
                HdfsDataUnit statsResult = calcStats(baseTable, profileTbl.toHdfsDataUnit("Profile"));
                statsResult.setName(baseRole + "Stats");
                statsTables.add(statsResult);
            } else {
                Preconditions.checkNotNull(reProfileAttrs, //
                        "Must save re-profile attrs list in " + reProfileAttrsKey);
                Preconditions.checkNotNull(changeListTbl, "Must have change list table " + changeListKey);
                // partial re-calculate
                HdfsDataUnit statsResult = calcStats(baseTable, profileTbl.toHdfsDataUnit("Profile"));
                statsResult.setName(baseRole + "Stats");
                statsTables.add(statsResult);
            }
        } else {
            log.info("No reason to re-calculate {} stats.", baseRole);
        }
    }

    private void mergeStats() {
        HdfsDataUnit statsData;
        if (statsTables.size() > 1) {
            UpsertConfig upsertConfig = new UpsertConfig();
            upsertConfig.setJoinKey(DataCloudConstants.PROFILE_ATTR_ATTRNAME);
            upsertConfig.setInput(statsTables);
            SparkJobResult result = runSparkJob(UpsertJob.class, upsertConfig);
            statsData = result.getTargets().get(0);
        } else {
            statsData = (HdfsDataUnit) statsTables.get(0);
        }
        statsTableName = NamingUtils.timestamp( "AccountStats");
        statsTbl = toTable(statsTableName, PROFILE_ATTR_ATTRNAME, statsData);
        metadataProxy.createTable(customerSpaceStr, statsTableName, statsTbl);
    }

    private boolean shouldDoNothing() {
        boolean doNothing;
        if (super.isToReset(getServingEntity())) {
            log.info("No need to calc stats for {}, as it is to be reset.", getServingEntity());
            doNothing = true;
        } else {
            customerAccountChanged = isChanged(ConsolidatedAccount, ACCOUNT_CHANGELIST_TABLE_NAME);
            Table latticeAccountTbl = attemptGetTableRole(LatticeAccount, false);
            if (latticeAccountTbl == null) {
                log.info("This tenant does not have lattice account table.");
                latticeAccountChanged = false;
            } else {
                latticeAccountChanged = isChanged(LatticeAccount, LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME);
            }
            doNothing = !(enforceRebuild || customerAccountChanged || latticeAccountChanged);
            log.info("customerAccountChanged={}, latticeAccountChanged={}, enforceRebuild={}, doNothing={}",
                    customerAccountChanged, latticeAccountChanged, enforceRebuild, doNothing);
        }
        return doNothing;
    }

    private void upsertStatsCube() {
        StatsCube cube = getStatsCube(statsTbl.toHdfsDataUnit("Stats"));
        String lockName = acquireStatsLock(CustomerSpace.shortenCustomerSpace(customerSpaceStr), inactive);
        try {
            Map<String, StatsCube> cubeMap = getCurrentCubeMap();
            if (cubeMap.containsKey(Account.name())) {
                StatsCube oldCube = cubeMap.get(Account.name());
                Map<String, AttributeStats> attrStats = new HashMap<>(oldCube.getStatistics());
                attrStats.putAll(cube.getStatistics());
                cube.setStatistics(attrStats);
            }
            cubeMap.put(Account.name(), cube);
            saveStatsContainer(cubeMap);
        } finally {
            LockManager.releaseWriteLock(lockName);
        }
    }

    // directly save active version stats to inactive version
    private void linkStatsContainer() {
        Map<String, StatsCube> cubeMap = getCurrentCubeMap();
        if (MapUtils.isNotEmpty(cubeMap)) {
            saveStatsContainer(cubeMap);
        } else {
            log.info("Skip saving an empty stats.");
        }
    }

    private void saveStatsContainer(Map<String, StatsCube> cubeMap) {
        StatisticsContainer statsContainer = new StatisticsContainer();
        statsContainer.setName(NamingUtils.timestamp("Stats"));
        statsContainer.setStatsCubes(cubeMap);
        statsContainer.setVersion(inactive);
        log.info("Saving stats with " + cubeMap.size() + " cubes.");
        dataCollectionProxy.upsertStats(customerSpaceStr, statsContainer);
    }

}
