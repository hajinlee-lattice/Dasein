package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LatticeAccountId;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.latticeengines.cdl.workflow.steps.BaseCalcStatsStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;

public abstract class UpdateProfileBase<T extends BaseProcessEntityStepConfiguration> extends BaseCalcStatsStep<T> {

    private static final Logger log = LoggerFactory.getLogger(UpdateProfileBase.class);

    protected abstract TableRoleInCollection getBaseTableRole();
    protected abstract List<String> getIncludeAttrs();
    protected abstract boolean getConsiderAMAttrs();
    protected abstract String getBaseChangeListCtxKey();
    protected abstract String getReProfileAttrsCtxKey();

    private TableRoleInCollection baseTableRole;
    private TableRoleInCollection profileRole;
    private String baseChangeListKey;

    @Override
    protected List<ProfileParameters.Attribute> getDeclaredAttrs() {
        List<ProfileParameters.Attribute> pAttrs = new ArrayList<>();
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(AccountId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(LatticeAccountId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(InterfaceName.CDLCreatedTime.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(InterfaceName.CDLUpdatedTime.name()));
        return pAttrs;
    }

    public void updateProfile() {
        baseTableRole = getBaseTableRole();
        profileRole = getProfileRole();
        baseChangeListKey = getBaseChangeListCtxKey();
        String reProfileAttrsKey = getReProfileAttrsCtxKey();
        String profileCtxKey = getProfileTableCtxKey();

        Table tblInCtx = getTableSummaryFromKey(customerSpace.toString(), profileCtxKey);
        if (tblInCtx != null) {
            log.info("Found {} in context, going thru short-cut mode.", profileCtxKey);
            dataCollectionProxy.upsertTable(customerSpaceStr, tblInCtx.getName(), profileRole, inactive);
        } else if (shouldDoNothing()) {
            linkInactiveTable(profileRole);
            String profileTableName = dataCollectionProxy.getTableName(customerSpaceStr, profileRole, inactive);
            putStringValueInContext(profileCtxKey, profileTableName);
        } else {
            Table baseTable = attemptGetTableRole(baseTableRole, true);
            List<String> includeAttrs = getIncludeAttrs();
            if (shouldRecalculateProfile()) {
                log.info("Should rebuild {}.", profileRole);
                log.info("considerAMAttrs={}", getConsiderAMAttrs());
                profile(baseTable, profileRole, //
                        profileCtxKey, includeAttrs, getDeclaredAttrs(), //
                        getConsiderAMAttrs(), true, true);
            } else {
                log.info("Should partial update {} using change list.", profileRole);
                log.info("considerAMAttrs={}", getConsiderAMAttrs());
                Table changeListTbl = getTableSummaryFromKey(customerSpaceStr, baseChangeListKey);
                Preconditions.checkNotNull(changeListTbl, "Must have base change list table");
                Table oldProfileTbl = attemptGetTableRole(profileRole, true);
                profileWithChangeList(baseTable, changeListTbl, oldProfileTbl, //
                        profileRole, profileCtxKey, reProfileAttrsKey, includeAttrs, getDeclaredAttrs(), //
                        getConsiderAMAttrs(), true, true);
            }
            String profileTableName = dataCollectionProxy.getTableName(customerSpaceStr, profileRole, inactive);
            putStringValueInContext(profileCtxKey, profileTableName);
        }
    }

    private boolean shouldDoNothing() {
        boolean hasBaseTableChange = isChanged(baseTableRole, baseChangeListKey);
        boolean enforceRebuild = getEnforceRebuild();
        boolean shouldRelinkProfile =  !enforceRebuild && !hasBaseTableChange;
        log.info("hasBaseTableChange={}, enforceRebuild={}, shouldRelinkProfile={}",
                hasBaseTableChange, enforceRebuild, shouldRelinkProfile);
        return shouldRelinkProfile;
    }

    protected boolean getEnforceRebuild() {
        return Boolean.TRUE.equals(configuration.getRebuild());
    }

    private boolean shouldRecalculateProfile() {
        boolean shouldRecalculate = false;
        if (attemptGetTableRole(profileRole, false) == null) {
            log.info("Should recalculate {}, because there was no {}} in active version.", profileRole, profileRole);
            shouldRecalculate = true;
        } else if (Boolean.TRUE.equals(configuration.getRebuild())) {
            log.info("Should recalculate {}, becaue enforced to rebuild {}.", //
                    profileRole, configuration.getMainEntity());
            shouldRecalculate = true;
        }
        return shouldRecalculate;
    }

}
