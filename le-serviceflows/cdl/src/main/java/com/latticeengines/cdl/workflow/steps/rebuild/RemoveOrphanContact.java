package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedContact;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.SortedContact;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.cdl.workflow.steps.BaseProcessAnalyzeSparkStep;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.RemoveOrphanConfig;
import com.latticeengines.domain.exposed.spark.common.GetRowChangesConfig;
import com.latticeengines.spark.exposed.job.cdl.RemoveOrphanJob;
import com.latticeengines.spark.exposed.job.common.GetRowChangesJob;

@Lazy
@Component("removeOrphanContact")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RemoveOrphanContact extends BaseProcessAnalyzeSparkStep<ProcessContactStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(RemoveOrphanContact.class);

    private Table contactTable;

    @Override
    public void execute() {
        bootstrap();
        contactTable = attemptGetTableRole(ConsolidatedContact, false);
        if (contactTable == null) {
            resetEntity(Contact);
        }
        if (isToReset(Contact)) {
            log.info("Should reset contact serving store, skip this step.");
        } else {
            if (shouldRefreshServingStore()) {
                Table tableInCtx = getTableSummaryFromKey(customerSpace.toString(), CONTACT_SERVING_TABLE_NAME);
                if (tableInCtx == null) {
                    RemoveOrphanConfig jobConfig = configureJob();
                    SparkJobResult result = runSparkJob(RemoveOrphanJob.class, jobConfig);
                    postJobExecution(result);
                } else {
                    log.info("Found contact serving table in context, skip this step.");
                    String servingTableName = tableInCtx.getName();
                    log.info("Link contact serving table {} to inactive version {}", servingTableName, inactive);
                    dataCollectionProxy.upsertTable(customerSpace.toString(), servingTableName, SortedContact,
                            inactive);
                    exportTableRoleToRedshift(tableInCtx, SortedContact);
                }
            } else {
                log.info("No need to refresh contact serving store.");
                linkInactiveTable(SortedContact);
            }
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
        String servingTableName = tenantId + "_" + NamingUtils.timestamp(Contact.name());
        Table servingTable = toTable(servingTableName, InterfaceName.ContactId.name(), result.getTargets().get(0));
        enrichTableSchema(servingTable);
        metadataProxy.createTable(tenantId, servingTableName, servingTable);
        dataCollectionProxy.upsertTable(tenantId, servingTableName, servingRole, inactive);
        exportTableRoleToRedshift(servingTable, servingRole);
        exportToS3AndAddToContext(servingTable, CONTACT_SERVING_TABLE_NAME);
    }

    private HdfsDataUnit getContactData() {
        TableRoleInCollection contactRole = Contact.getBatchStore();
        Preconditions.checkNotNull(contactTable, "Must have ConsolidatedContact table.");
        return contactTable.toHdfsDataUnit("Contact");
    }

    private HdfsDataUnit getAccountData() {
        TableRoleInCollection accountRole = Account.getBatchStore();
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
        if (isChanged(ConsolidatedAccount)) { // has potential changes
            Table changeList = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_CHANGELIST_TABLE_NAME);
            if (changeList == null) {
                log.info("No account change list to make more accurate decision, " +
                        "going to rebuild conatct serving store regardlessly.");
                return true;
            } else {
                GetRowChangesConfig jobConfig = new GetRowChangesConfig();
                jobConfig.setInput(Collections.singletonList(changeList.toHdfsDataUnit("AccountChange")));
                SparkJobResult result = runSparkJob(GetRowChangesJob.class, jobConfig);
                long newAcc = result.getTargets().get(0).getCount();
                long delAcc = result.getTargets().get(1).getCount();
                log.info("There are {} new accounts and {} deleted accounts", newAcc, delAcc);
                return (newAcc > 0) || (delAcc > 0);
            }
        } else {
            return false;
        }
    }

    private void enrichTableSchema(Table table) {
        Map<String, Attribute> masterAttrs = new HashMap<>();
        contactTable.getAttributes().forEach(attr -> {
            masterAttrs.put(attr.getName(), attr);
        });
        List<Attribute> attrs = new ArrayList<>();
        final AtomicLong masterCount = new AtomicLong(0);
        table.getAttributes().forEach(attr0 -> {
            Attribute attr = copyMasterAttr(masterAttrs, attr0);
            if (masterAttrs.containsKey(attr0.getName())) {
                attr = copyMasterAttr(masterAttrs, attr0);
                if (LogicalDataType.Date.equals(attr0.getLogicalDataType())) {
                    log.info("Setting last data refresh for contact date attribute: " + attr.getName() + " to "
                            + evaluationDateStr);
                    attr.setLastDataRefresh("Last Data Refresh: " + evaluationDateStr);
                }
                masterCount.incrementAndGet();
            }
            // update metadata for AccountId attribute since it is only created after lead
            // to account match and does not have the correct metadata
            if (configuration.isEntityMatchEnabled() && InterfaceName.AccountId.name().equals(attr.getName())) {
                attr.setInterfaceName(InterfaceName.AccountId);
                attr.setTags(Tag.INTERNAL);
                attr.setLogicalDataType(LogicalDataType.Id);
                attr.setNullable(false);
                attr.setApprovedUsage(ApprovedUsage.NONE);
                attr.setSourceLogicalDataType(attr.getPhysicalDataType());
                attr.setFundamentalType(FundamentalType.ALPHA.getName());
            }
            attr.setCategory(Category.CONTACT_ATTRIBUTES);
            attr.setSubcategory(null);
            attr.removeAllowedDisplayNames();
            attrs.add(attr);
        });
        table.setAttributes(attrs);
        log.info("Copied " + masterCount.get() + " attributes from batch store metadata.");
    }

}
