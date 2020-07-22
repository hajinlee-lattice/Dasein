package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ContactProfile;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.SortedContact;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.BaseCalcStatsStep;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

@Lazy
@Component("calcContactStats")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalcContactStats extends BaseCalcStatsStep<ProcessContactStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CalcContactStats.class);

    private boolean enforceRebuild;
    private boolean contactChanged;

    @Override
    protected TableRoleInCollection getProfileRole() {
        return ContactProfile;
    }

    @Override
    protected String getProfileTableCtxKey() {
        return CONTACT_PROFILE_TABLE_NAME;
    }

    @Override
    protected String getStatsTableCtxKey() {
        return CONTACT_STATS_TABLE_NAME;
    }

    @Override
    protected String getStatsUpdatedFlagCtxKey() {
        return CONTACT_STATS_UPDATED;
    }

    @Override
    public void execute() {
        prepare();
        if (isToReset(getServingEntity())) {
            log.info("Need to remove calc stats for {}, as it is to be reset.", getServingEntity());
            cleanupStatsCube();
        } else {
            if (shouldDoNothing()) {
                log.info("No need to update Account stats.");
                linkStatsContainer();
            } else {
                statsTbl = getTableSummaryFromKey(customerSpaceStr, getStatsTableCtxKey());
                statsDiffTbl = getTableSummaryFromKey(customerSpaceStr, CONTACT_STATS_DIFF_TABLE_NAME);
                if (statsTbl == null && statsDiffTbl == null) {
                    updateContactStats();
                    mergeStats();
                    mergeStatsDiff();

                    // for retry
                    if (statsTbl != null) {
                        exportToS3AndAddToContext(statsTbl, getStatsTableCtxKey());
                    }
                    if (statsDiffTbl != null) {
                        exportToS3AndAddToContext(statsDiffTbl, CONTACT_STATS_DIFF_TABLE_NAME);
                    }
                }
                upsertStatsCube();
            }
        }
    }

    private void updateContactStats() {
        updateStats(contactChanged, enforceRebuild, SortedContact, ContactProfile, //
                CONTACT_RE_PROFILE_ATTRS, CONTACT_CHANGELIST_TABLE_NAME);
    }

    private boolean shouldDoNothing() {
        boolean doNothing;
        enforceRebuild = Boolean.TRUE.equals(configuration.getRebuild());
        contactChanged = isChanged(SortedContact, CONTACT_CHANGELIST_TABLE_NAME);
        boolean profileChanged = isChanged(ContactProfile);
        doNothing = !(enforceRebuild || profileChanged || contactChanged);
        log.info("enforceRebuild={}, contactChanged={}, profileChanged={}, doNothing={}", //
                enforceRebuild, contactChanged, profileChanged, doNothing);
        return doNothing;
    }

}
