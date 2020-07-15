package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityId;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ContactProfile;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.SortedContact;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

@Lazy
@Component("updateContactProfile")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateContactProfile extends UpdateProfileBase<ProcessContactStepConfiguration> {

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Override
    protected TableRoleInCollection getBaseTableRole() {
        return SortedContact;
    }

    @Override
    protected List<String> getIncludeAttrs() {
        List<String> retainAttrNames = servingStoreProxy
                .getDecoratedMetadata(customerSpace.toString(), BusinessEntity.Contact, null, inactive) //
                .filter(cm -> !AttrState.Inactive.equals(cm.getAttrState())) //
                .map(ColumnMetadata::getAttrName) //
                .collectList().block();
        if (retainAttrNames == null) {
            retainAttrNames = new ArrayList<>();
        }
        if (!retainAttrNames.contains(InterfaceName.ContactId.name())) {
            retainAttrNames.add(InterfaceName.ContactId.name());
        }
        return retainAttrNames;
    }

    @Override
    protected List<ProfileParameters.Attribute> getDeclaredAttrs() {
        List<ProfileParameters.Attribute> pAttrs = new ArrayList<>();
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(EntityId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(AccountId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(ContactId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(CustomerAccountId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(CustomerContactId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(InterfaceName.CDLCreatedTime.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(InterfaceName.CDLUpdatedTime.name()));
        return pAttrs;
    }

    @Override
    protected String getBaseChangeListCtxKey() {
        return CONTACT_CHANGELIST_TABLE_NAME;
    }

    @Override
    protected String getReProfileAttrsCtxKey() {
        return CONTACT_RE_PROFILE_ATTRS;
    }

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
    public void execute() {
        bootstrap();
        autoDetectCategorical = true;
        autoDetectDiscrete = true;
        updateProfile();
    }

}
