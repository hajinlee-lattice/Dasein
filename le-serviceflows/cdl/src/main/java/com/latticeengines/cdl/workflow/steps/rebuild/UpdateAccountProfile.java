package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.LatticeAccountId;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountProfile;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.BucketedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccount;

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

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

@Lazy
@Component("updateAccountProfile")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateAccountProfile extends UpdateProfileBase<ProcessAccountStepConfiguration> {

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Override
    protected TableRoleInCollection getBaseTableRole() {
        return ConsolidatedAccount;
    }

    @Override
    protected List<String> getIncludeAttrs() {
        List<String> includeAttrs = getRetrainAttrNames();
        includeAttrs.remove(LatticeAccountId.name());
        return includeAttrs;
    }

    @Override
    protected String getBaseChangeListCtxKey() {
        return ACCOUNT_CHANGELIST_TABLE_NAME;
    }

    @Override
    protected String getReProfileAttrsCtxKey() {
        return ACCOUNT_RE_PROFILE_ATTRS;
    }

    @Override
    protected TableRoleInCollection getProfileRole() {
        return AccountProfile;
    }

    @Override
    protected String getProfileTableCtxKey() {
        return ACCOUNT_PROFILE_TABLE_NAME;
    }

    @Override
    protected String getStatsTableCtxKey() {
        return ACCOUNT_STATS_TABLE_NAME;
    }

    @Override
    public void execute() {
        bootstrap();
        autoDetectCategorical = true;
        autoDetectDiscrete = true;
        updateProfile();
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
    protected boolean hasNewAttrs() {
        Set<String> newAttrs = new HashSet<>(includeAttrs);
        Table servingStore = attemptGetTableRole(BucketedAccount, false);
        if (servingStore != null) {
            newAttrs.removeAll(Arrays.asList(servingStore.getAttributeNames()));
        }
        Table latticeAccount = attemptGetTableRole(LatticeAccount, false);
        if (latticeAccount != null) {
            newAttrs.removeAll(Arrays.asList(latticeAccount.getAttributeNames()));
        }
        return !newAttrs.isEmpty();
    }

}
