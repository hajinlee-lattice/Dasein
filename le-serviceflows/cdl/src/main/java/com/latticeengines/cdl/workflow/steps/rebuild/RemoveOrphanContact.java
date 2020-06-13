package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedContact;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.SortedContact;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.cdl.workflow.steps.BaseProcessAnalyzeSparkStep;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.RemoveOrphanConfig;
import com.latticeengines.spark.exposed.job.cdl.RemoveOrphanJob;

@Lazy
@Component("removeOrphanContact")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RemoveOrphanContact extends BaseProcessAnalyzeSparkStep<ProcessContactStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(RemoveOrphanContact.class);

    @Inject
    private BatonService batonService;

    @Override
    public void execute() {
        bootstrap();
        if (shouldRefreshServingStore()) {
            Table tableInCtx = getTableSummaryFromKey(customerSpace.toString(), CONTACT_SERVING_TABLE_NAME);
            if (tableInCtx == null) {
                RemoveOrphanConfig jobConfig = configureJob();
                SparkJobResult result = runSparkJob(RemoveOrphanJob.class, jobConfig);
                postJobExecution(result);
            } else {
                log.info("Found contact serving table in context, skip this step.");
            }
        } else {
            log.info("No need to refresh contact serving store.");
            linkInactiveTable(SortedContact);
        }
    }

    private RemoveOrphanConfig configureJob() {
        HdfsDataUnit accountData = getAccountData();
        HdfsDataUnit contactData = getContactData();

        RemoveOrphanConfig jobConfig = new RemoveOrphanConfig();
        jobConfig.setParentId(InterfaceName.AccountId.name());

        List<DataUnit> input = new ArrayList<>();
        input.add(contactData);
        if (accountData != null) {
            input.add(accountData);
        }
        jobConfig.setInput(input);

        if (accountData != null) {
            jobConfig.setParentSrcIdx(1);
        } else {
            jobConfig.setParentSrcIdx(-1);
        }
        return jobConfig;
    }

    private void postJobExecution(SparkJobResult result) {
        String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
        TableRoleInCollection servingRole = SortedContact;
        String servingTableName = tenantId + "_" + NamingUtils.timestamp(servingRole.name());
        Table servingTable = toTable(servingTableName, InterfaceName.ContactId.name(), result.getTargets().get(0));
        metadataProxy.createTable(tenantId, servingTableName, servingTable);
        dataCollectionProxy.upsertTable(tenantId, servingTableName, servingRole, inactive);
        exportToS3AndAddToContext(servingTable, CONTACT_SERVING_TABLE_NAME);
    }

    private HdfsDataUnit getContactData() {
        TableRoleInCollection contactRole = BusinessEntity.Contact.getBatchStore();
        Table contactTable = attemptGetTableRole(contactRole, true);
        return contactTable.toHdfsDataUnit("Contact");
    }

    private HdfsDataUnit getAccountData() {
        TableRoleInCollection accountRole = BusinessEntity.Account.getBatchStore();
        Table accountTable = attemptGetTableRole(accountRole, false);
        if (accountTable == null) {
            log.info("No Account batch store, all contacts are non-orphan.");
            return null;
        } else {
            return accountTable.toHdfsDataUnit("Account");
        }
    }

    private boolean shouldRefreshServingStore() {
        boolean shouldRefresh = false;
        if (isChanged(ConsolidatedContact)) {
            log.info("Should refresh contact serving store, because there are changes to contact batch store.");
            shouldRefresh = true;
        } else if (hasAccountChange()) {
            log.info("Should refresh contact serving store, because there are account changes.");
            shouldRefresh = true;
        }
        return shouldRefresh;
    }

    // has account change that might impact orphan contact
    private boolean hasAccountChange() {
        if (isATTHotFix()) {
            return hasAccountDeletion();
        } else {
            TableRoleInCollection accountBatchStore = TableRoleInCollection.ConsolidatedAccount;
            return isChanged(accountBatchStore);
        }
    }

    private boolean hasAccountDeletion() {
        List<Action> softDeletes = getListObjectFromContext(SOFT_DELETE_ACTIONS, Action.class);
        boolean hasAccountSoftDeletion = CollectionUtils.isNotEmpty(softDeletes) && softDeletes.stream().anyMatch(action -> {
            DeleteActionConfiguration configuration = (DeleteActionConfiguration) action.getActionConfiguration();
            return configuration.hasEntity(BusinessEntity.Account);
        });

        Set<Action> actions = getSetObjectFromContext(ACCOUNT_LEGACY_DELTE_BYUOLOAD_ACTIONS, Action.class);
        boolean hasLegacyAccountDeletion = CollectionUtils.isNotEmpty(actions);
        return hasLegacyAccountDeletion && hasAccountSoftDeletion;
    }

    private boolean isATTHotFix() {
        return batonService.shouldSkipFuzzyMatchInPA(customerSpace.getTenantId());
    }

}
