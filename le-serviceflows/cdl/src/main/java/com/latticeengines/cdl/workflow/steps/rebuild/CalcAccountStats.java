package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountProfile;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccountProfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.BaseCalcStatsStep;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;

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

    @Override
    protected String getStatsUpdatedFlagCtxKey() {
        return ACCOUNT_STATS_UPDATED;
    }

    @Override
    protected long getBaseTableCount() {
        Table baseTbl = attemptGetTableRole(ConsolidatedAccount, true);
        return baseTbl.toHdfsDataUnit("Base").getCount();
    }

    private boolean enforceRebuild;
    private boolean customerAccountChanged;
    private boolean latticeAccountChanged;

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
                mergeStatsDiff();

                // for retry
                if (statsTbl != null) {
                    exportToS3AndAddToContext(statsTbl, getStatsTableCtxKey());
                }
                if (statsDiffTbl != null) {
                    exportToS3AndAddToContext(statsDiffTbl, ACCOUNT_STATS_DIFF_TABLE_NAME);
                }
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

}
