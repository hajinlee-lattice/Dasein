package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityId;
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
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;

public abstract class UpdateProfileBase<T extends BaseProcessEntityStepConfiguration> extends BaseCalcStatsStep<T> {

    private static final Logger log = LoggerFactory.getLogger(UpdateProfileBase.class);

    protected abstract TableRoleInCollection getBaseTableRole();
    protected abstract List<String> getIncludeAttrs();
    protected abstract String getBaseChangeListCtxKey();
    protected abstract String getReProfileAttrsCtxKey();
    protected abstract boolean hasNewAttrs();

    private TableRoleInCollection baseTableRole;
    private TableRoleInCollection profileRole;
    private String baseChangeListKey;
    protected boolean ignoreDateAttrs;
    protected boolean considerAMAttrs;
    List<String> includeAttrs;

    @Override
    protected List<ProfileParameters.Attribute> getDeclaredAttrs() {
        List<ProfileParameters.Attribute> pAttrs = new ArrayList<>();
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(EntityId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(AccountId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(CustomerAccountId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(LatticeAccountId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(InterfaceName.CDLCreatedTime.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(InterfaceName.CDLUpdatedTime.name()));
        return pAttrs;
    }

    public void updateProfile() {
        BusinessEntity servingEntity = getServingEntity();
        if (isToReset(servingEntity)) {
            log.info("Should reset {}, skip this step", servingEntity);
            return;
        }

        baseTableRole = getBaseTableRole();
        profileRole = getProfileRole();
        baseChangeListKey = getBaseChangeListCtxKey();
        String profileCtxKey = getProfileTableCtxKey();
        includeAttrs = getIncludeAttrs();

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
            if (shouldRecalculateProfile()) {
                log.info("Should rebuild {}.", profileRole);
                log.info("considerAMAttrs={}", considerAMAttrs);
                profile(baseTable, profileRole, profileCtxKey, includeAttrs, getDeclaredAttrs(), //
                        considerAMAttrs, true, true);
            } else {
                log.info("Should partial update {} using change list.", profileRole);
                log.info("considerAMAttrs={}", considerAMAttrs);
                Table changeListTbl = getTableSummaryFromKey(customerSpaceStr, baseChangeListKey);
                Preconditions.checkNotNull(changeListTbl, "Must have base change list table");
                Table oldProfileTbl = attemptGetTableRole(profileRole, true);
                profileWithChangeList(baseTable, changeListTbl, oldProfileTbl, //
                        profileRole, profileCtxKey, getReProfileAttrsCtxKey(), includeAttrs, getDeclaredAttrs(), //
                        considerAMAttrs, ignoreDateAttrs,true, true);
            }
            String profileTableName = dataCollectionProxy.getTableName(customerSpaceStr, profileRole, inactive);
            putStringValueInContext(profileCtxKey, profileTableName);
        }
    }

    private boolean shouldDoNothing() {
        boolean hasBaseTableChange = isChanged(baseTableRole, baseChangeListKey);
        boolean enforceRebuild = getEnforceRebuild();
        boolean hasOldProfile= attemptGetTableRole(profileRole, false) != null;
        boolean shouldRelinkProfile = !enforceRebuild && !hasBaseTableChange && hasOldProfile;
        log.info("hasBaseTableChange={}, enforceRebuild={}, hasOldProfile={}, shouldRelinkProfile={}",
                hasBaseTableChange, enforceRebuild, hasOldProfile, shouldRelinkProfile);
        if (shouldRelinkProfile) {
            boolean hasNewAttrs = hasNewAttrs();
            shouldRelinkProfile = !hasNewAttrs;
            log.info("hasNewAttrs={}, shouldRelinkProfile={}", hasNewAttrs, shouldRelinkProfile);
        }
        return shouldRelinkProfile;
    }

    protected boolean getEnforceRebuild() {
        return Boolean.TRUE.equals(configuration.getRebuild());
    }

    private boolean shouldRecalculateProfile() {
        boolean shouldRecalculate = false;
        if (attemptGetTableRole(profileRole, false) == null) {
            log.info("Should recalculate {}, because there was no {} in active version.", profileRole, profileRole);
            shouldRecalculate = true;
        } else if (Boolean.TRUE.equals(configuration.getRebuild())) {
            log.info("Should recalculate {}, because enforced to rebuild {}.", //
                    profileRole, configuration.getMainEntity());
            shouldRecalculate = true;
        } else {
            Table changeListTbl = getTableSummaryFromKey(customerSpaceStr, baseChangeListKey);
            if (changeListTbl == null) {
                log.info("Should recalculate {}, because no change list table.", profileRole);
                shouldRecalculate = true;
            }
        }
        return shouldRecalculate;
    }

}
