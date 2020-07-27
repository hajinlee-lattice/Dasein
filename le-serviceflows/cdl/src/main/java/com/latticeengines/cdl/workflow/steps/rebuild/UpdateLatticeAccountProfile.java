package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.BucketedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccountProfile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

@Lazy
@Component("updateLatticeAccountProfile")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateLatticeAccountProfile extends UpdateProfileBase<ProcessAccountStepConfiguration> {

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private BatonService batonService;

    @Override
    protected TableRoleInCollection getBaseTableRole() {
        return LatticeAccount;
    }

    @Override
    protected List<String> getIncludeAttrs() {
        List<String> includeAttrs = getRetrainAttrNames();
        Table customerAccount = attemptGetTableRole(ConsolidatedAccount, true);
        includeAttrs.removeAll(Arrays.asList(customerAccount.getAttributeNames()));
        includeAttrs.add(AccountId.name());
        return includeAttrs;
    }

    @Override
    protected String getBaseChangeListCtxKey() {
        return LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME;
    }

    @Override
    protected String getReProfileAttrsCtxKey() {
        return LATTICE_ACCOUNT_RE_PROFILE_ATTRS;
    }

    @Override
    protected TableRoleInCollection getProfileRole() {
        return LatticeAccountProfile;
    }

    @Override
    protected String getProfileTableCtxKey() {
        return LATTICE_ACCOUNT_PROFILE_TABLE_NAME;
    }

    @Override
    protected String getStatsTableCtxKey() {
        return ACCOUNT_STATS_TABLE_NAME;
    }

    @Override
    public void execute() {
        bootstrap();
        boolean noLDC = batonService.shouldExcludeDataCloudAttrs(customerSpace.getTenantId());
        if (!noLDC) {
            autoDetectCategorical = true;
            autoDetectDiscrete = true;
            considerAMAttrs = true;
            ignoreDateAttrs = true;
            updateProfile();
        }
    }

    private List<String> getRetrainAttrNames() {
        List<String> retainAttrNames = servingStoreProxy
                .getDecoratedMetadata(customerSpace.toString(), BusinessEntity.Account, null, inactive) //
                .filter(cm -> !AttrState.Inactive.equals(cm.getAttrState())) //
                .filter(cm -> !Boolean.FALSE.equals(cm.getCanSegment())) //
                .map(ColumnMetadata::getAttrName) //
                .collectList().block();
        if (retainAttrNames == null) {
            retainAttrNames = new ArrayList<>();
        }
        if (!retainAttrNames.contains(InterfaceName.LatticeAccountId.name())) {
            retainAttrNames.add(InterfaceName.LatticeAccountId.name());
        }
        if (!retainAttrNames.contains(InterfaceName.AccountId.name())) {
            retainAttrNames.add(InterfaceName.AccountId.name());
        }
        if (!retainAttrNames.contains(InterfaceName.CDLUpdatedTime.name())) {
            retainAttrNames.add(InterfaceName.CDLUpdatedTime.name());
        }
        return retainAttrNames;
    }

    @Override
    protected boolean getEnforceRebuild() {
        return Boolean.TRUE.equals(configuration.getRebuild()) && //
                Boolean.TRUE.equals(getObjectFromContext(REBUILD_LATTICE_ACCOUNT, Boolean.class));
    }

    @Override
    protected boolean hasNewAttrs() {
        Table latticeAccount = attemptGetTableRole(LatticeAccount, false);
        if (latticeAccount != null) {
            Set<String> newAttrs = new HashSet<>(includeAttrs);
            Table servingStore = attemptGetTableRole(BucketedAccount, false);
            if (servingStore != null) {
                newAttrs.removeAll(Arrays.asList(servingStore.getAttributeNames()));
            }
            Table customerAccount = attemptGetTableRole(ConsolidatedAccount, true);
            newAttrs.removeAll(Arrays.asList(customerAccount.getAttributeNames()));
            return !newAttrs.isEmpty();
        } else {
            return false;
        }
    }

}
