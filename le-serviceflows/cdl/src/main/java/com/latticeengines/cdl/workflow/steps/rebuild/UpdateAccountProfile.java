package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ATTRNAME;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LatticeAccountId;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountProfile;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccountProfile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.cdl.workflow.steps.BaseCalcStatsStep;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.UpsertConfig;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.spark.exposed.job.common.UpsertJob;

@Lazy
@Component("updateAccountProfile")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateAccountProfile extends BaseCalcStatsStep<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(UpdateAccountProfile.class);

    private List<String> segmentAttrs;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Override
    public void execute() {
        prepare();
        autoDetectCategorical = true;
        autoDetectDiscrete = true;
        if (shouldRecalculate()) {
            recalculate();
        } else {
            syncProfileTables();
        }
    }

    private void recalculate() {
        if (StringUtils.isBlank(statsTableName)) {
            segmentAttrs = getRetrainAttrNames();
            HdfsDataUnit customerStats = updateCustomerStats();
            HdfsDataUnit latticeStats = updateLatticeStats();
            mergeStats(customerStats, latticeStats);
        } else {
            syncProfileTables();
        }
        updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);
    }

    private HdfsDataUnit updateCustomerStats() {
        Table customerAccount = attemptGetTableRole(ConsolidatedAccount, true);
        HdfsDataUnit customerProfile = profileCustomerAccount(customerAccount);
        return calcStats(customerAccount, customerProfile);
    }

    private HdfsDataUnit updateLatticeStats() {
        Table latticeAccount = attemptGetTableRole(LatticeAccount, true);
        HdfsDataUnit latticeProfile = profileLatticeAccount(latticeAccount);
        return calcStats(latticeAccount, latticeProfile);
    }

    private HdfsDataUnit profileCustomerAccount(Table customerAccount) {
        Table tblInCtx = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_PROFILE_TABLE_NAME);
        if (tblInCtx != null) {
            log.info("Found ACCOUNT_PROFILE_TABLE_NAME in context, going thru short-cut mode.");
            dataCollectionProxy.upsertTable(customerSpaceStr, tblInCtx.getName(), AccountProfile, inactive);
            return tblInCtx.toHdfsDataUnit("AccountProfile");
        } else {
            List<String> includeAttrs = new ArrayList<>(segmentAttrs);
            includeAttrs.remove(LatticeAccountId.name());
            if (shouldRecalculateCustomerProfile()) {
                log.info("Should rebuild customer account profile.");
                return profile(customerAccount, AccountProfile, //
                        ACCOUNT_PROFILE_TABLE_NAME, includeAttrs, getDeclaredAttrs(), //
                        false, true, true);
            } else {
                log.info("Should partial update customer account profile using change list.");
                Table changeListTbl = getTableSummaryFromKey(customerSpaceStr, ACCOUNT_CHANGELIST_TABLE_NAME);
                Preconditions.checkNotNull(changeListTbl, "Must have lattice account change list table");
                Table oldProfileTbl = attemptGetTableRole(AccountProfile, true);
                return profileWithChangeList(customerAccount, changeListTbl, oldProfileTbl, //
                        AccountProfile, ACCOUNT_PROFILE_TABLE_NAME, includeAttrs, getDeclaredAttrs(), //
                        false, true, true);
            }
        }
    }

    private boolean shouldRecalculateCustomerProfile() {
        boolean shouldRecalculate = false;
        if (attemptGetTableRole(AccountProfile, false) == null) {
            log.info("Should recalculate customer account profile, " + //
                    "because there was no AccountProfile in active version.");
            shouldRecalculate = true;
        }
        return shouldRecalculate;
    }

    private HdfsDataUnit profileLatticeAccount(Table latticeAccount) {
        Table tblInCtx = getTableSummaryFromKey(customerSpace.toString(), LATTICE_ACCOUNT_PROFILE_TABLE_NAME);
        if (tblInCtx != null) {
            log.info("Found LATTICE_ACCOUNT_PROFILE_TABLE_NAME in context, going thru short-cut mode.");
            dataCollectionProxy.upsertTable(customerSpaceStr, tblInCtx.getName(), LatticeAccountProfile, inactive);
            return tblInCtx.toHdfsDataUnit("LatticeProfile");
        } else {
            List<String> includeAttrs = new ArrayList<>(segmentAttrs);
            Table customerAccount = attemptGetTableRole(ConsolidatedAccount, true);
            includeAttrs.removeAll(Arrays.asList(customerAccount.getAttributeNames()));
            includeAttrs.add(AccountId.name());
            if (shouldRecalculateLatticeProfile()) {
                log.info("Should rebuild lattice account profile.");
                return profile(latticeAccount, LatticeAccountProfile, //
                        LATTICE_ACCOUNT_PROFILE_TABLE_NAME, includeAttrs, getDeclaredAttrs(), //
                        true, true, true);
            } else {
                log.info("Should partial update lattice account profile using change list.");
                Table changeListTbl = getTableSummaryFromKey(customerSpaceStr, LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME);
                Preconditions.checkNotNull(changeListTbl, "Must have lattice account change list table");
                Table oldProfileTbl = attemptGetTableRole(LatticeAccountProfile, true);
                return profileWithChangeList(latticeAccount, changeListTbl, oldProfileTbl, //
                        LatticeAccountProfile, LATTICE_ACCOUNT_PROFILE_TABLE_NAME, includeAttrs, getDeclaredAttrs(), //
                        true, true, true);
            }
        }
    }

    private boolean shouldRecalculateLatticeProfile() {
        boolean shouldRecalculate = false;
        if (Boolean.TRUE.equals(getObjectFromContext(REBUILD_LATTICE_ACCOUNT, Boolean.class))) {
            log.info("Should recalculate lattice account profile, " + //
                    "because REBUILD_LATTICE_ACCOUNT is true.");
            shouldRecalculate = true;
        } else if (attemptGetTableRole(LatticeAccountProfile, false) == null) {
            log.info("Should recalculate lattice account profile, " + //
                    "because there was no LatticeAccountProfile in active version.");
            shouldRecalculate = true;
        }
        return shouldRecalculate;
    }

    @Override
    protected List<ProfileParameters.Attribute> getDeclaredAttrs() {
        List<ProfileParameters.Attribute> pAttrs = new ArrayList<>();
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(AccountId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(LatticeAccountId.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(InterfaceName.CDLCreatedTime.name()));
        pAttrs.add(ProfileParameters.Attribute.nonBktAttr(InterfaceName.CDLUpdatedTime.name()));
        return pAttrs;
    }

    private void mergeStats(HdfsDataUnit customerStats, HdfsDataUnit latticeStats) {
        UpsertConfig upsertConfig = new UpsertConfig();
        upsertConfig.setJoinKey(DataCloudConstants.PROFILE_ATTR_ATTRNAME);
        upsertConfig.setInput(Arrays.asList(latticeStats, customerStats));
        SparkJobResult result = runSparkJob(UpsertJob.class, upsertConfig);
        statsTableName = NamingUtils.timestamp( "AccountStats");
        Table statsTable = toTable(statsTableName, PROFILE_ATTR_ATTRNAME, result.getTargets().get(0));
        metadataProxy.createTable(customerSpaceStr, statsTableName, statsTable);
        exportToS3AndAddToContext(statsTable, getStatsTableCtxKey());
    }

    private boolean shouldRecalculate() {
        boolean should;
        if (super.isToReset(getServingEntity())) {
            log.info("No need to calc stats for {}, as it is to be reset.", getServingEntity());
            should = false;
        } else {
            boolean customerAccountChanged = isChanged(ConsolidatedAccount);
            boolean latticeAccountChanged = isChanged(LatticeAccount);
            should = customerAccountChanged || latticeAccountChanged;
            log.info("customerAccountChanged={}, latticeAccountChanged={}, shouldRecalculate={}",
                    customerAccountChanged, latticeAccountChanged, should);
        }
        return should;
    }

    private void syncProfileTables() {
        linkInactiveTable(AccountProfile);
        linkInactiveTable(LatticeAccountProfile);
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

}
