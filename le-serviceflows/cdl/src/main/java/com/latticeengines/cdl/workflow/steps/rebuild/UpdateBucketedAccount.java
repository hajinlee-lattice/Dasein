package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountProfile;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.BucketedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccountProfile;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.BaseProcessAnalyzeSparkStep;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.DCEncodedAttr;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.JoinAccountStoresConfig;
import com.latticeengines.domain.exposed.spark.stats.BucketEncodeConfig;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.spark.exposed.job.cdl.JoinAccountStores;
import com.latticeengines.spark.exposed.job.stats.BucketEncodeJob;
import com.latticeengines.spark.exposed.utils.BucketEncodeUtils;

@Lazy
@Component("updateBucketedAccount")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateBucketedAccount extends BaseProcessAnalyzeSparkStep<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(UpdateBucketedAccount.class);

    private Table customerAccountTbl;
    private Table customerProfileTbl;
    private Table latticeAccountTbl;
    private Table latticeProfileTbl;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Override
    public void execute() {
        bootstrap();
        Table servingTable = getTableSummaryFromKey(customerSpaceStr, ACCOUNT_SERVING_TABLE_NAME);
        if (servingTable != null) {
            log.info("Found account serving store in context, going through short-cut mode.");
        } else {
            latticeAccountTbl = attemptGetTableRole(LatticeAccount, false);
            if (shouldRunEncode()) {
                latticeProfileTbl = attemptGetTableRole(LatticeAccountProfile, false);
                customerAccountTbl = attemptGetTableRole(ConsolidatedAccount, true);
                customerProfileTbl = attemptGetTableRole(AccountProfile, true);
                HdfsDataUnit customerEncode = encode(customerAccountTbl.toHdfsDataUnit("Data"),
                        customerProfileTbl.toHdfsDataUnit("Profile"));
                HdfsDataUnit mergeEncode;
                if (latticeAccountTbl == null) {
                    log.info("This tenant does not have LatticeAccount data.");
                    mergeEncode = customerEncode;
                } else {
                    HdfsDataUnit latticeEncode = encode(latticeAccountTbl.toHdfsDataUnit("Data"),
                            latticeProfileTbl.toHdfsDataUnit("Profile"));
                    mergeEncode = merge(customerEncode, latticeEncode);
                }
                String tenantId = CustomerSpace.shortenCustomerSpace(customerSpaceStr);
                String servingTableName = tenantId + "_" + NamingUtils.timestamp(Account.name());
                servingTable = toTable(servingTableName, AccountId.name(), mergeEncode);
                expandEncAttrs(servingTable);
                enrichTableSchema(servingTable);
                enrichCustomerAccountTableSchema(customerAccountTbl);
                metadataProxy.createTable(customerSpaceStr, servingTableName, servingTable);
                dataCollectionProxy.upsertTable(customerSpaceStr, servingTableName, BucketedAccount, inactive);
                exportToS3AndAddToContext(servingTable, ACCOUNT_SERVING_TABLE_NAME);
            } else {
                linkInactiveTable(BucketedAccount);
                String tableName = dataCollectionProxy.getTableName(customerSpaceStr, BucketedAccount, inactive);
                putStringValueInContext(ACCOUNT_SERVING_TABLE_NAME, tableName);
            }
        }
        // regardless short-cut or not, need to publish to redshift
        if (isChanged(BucketedAccount)) {
            exportTableRoleToRedshift(servingTable, BucketedAccount);
        }
    }

    private HdfsDataUnit encode(HdfsDataUnit inputData, HdfsDataUnit profileData) {
        BucketEncodeConfig config = new BucketEncodeConfig();
        config.setInput(Arrays.asList(inputData, profileData));

        String profileAvroGlob = PathUtils.toAvroGlob(((HdfsDataUnit) config.getInput().get(1)).getPath());
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, profileAvroGlob);
        config.setEncAttrs(BucketEncodeUtils.encodedAttrs(records));
        List<String> retainAttrs = BucketEncodeUtils.retainFields(records);
        retainAttrs.retainAll(BucketEncodeUtils.retainFields(records));
        config.setRetainAttrs(retainAttrs);
        config.setRenameFields(BucketEncodeUtils.renameFields(records));

        SparkJobResult result = runSparkJob(BucketEncodeJob.class, config);
        return result.getTargets().get(0);
    }

    private HdfsDataUnit merge(HdfsDataUnit customerEncode, HdfsDataUnit latticeEncode) {
        JoinAccountStoresConfig config = new JoinAccountStoresConfig();
        config.setInput(Arrays.asList(customerEncode, latticeEncode));
        SparkJobResult result = runSparkJob(JoinAccountStores.class, config);
        return result.getTargets().get(0);
    }

    private boolean shouldRunEncode() {
        boolean shouldRunEncode = false;
        if (Boolean.TRUE.equals(configuration.getRebuild())) {
            log.info("Should run encode, because enforced to rebuild");
            shouldRunEncode = true;
        } else if (Boolean.TRUE.equals(getObjectFromContext(REBUILD_LATTICE_ACCOUNT, Boolean.class))) {
            log.info("Should run encode, because upstream marked REBUILD_LATTICE_ACCOUNT to true");
            shouldRunEncode = true;
        } else if (isChanged(ConsolidatedAccount, ACCOUNT_CHANGELIST_TABLE_NAME)) {
            log.info("Should run encode, because customer account has changes");
            shouldRunEncode = true;
        } else if (isChanged(AccountProfile)) {
            log.info("Should run encode, because account profile has changes");
            shouldRunEncode = true;
        } else if (latticeAccountTbl != null && isChanged(LatticeAccount, LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME)) {
            log.info("Should run encode, because lattice account has changes");
            shouldRunEncode = true;
        } else if (latticeAccountTbl != null && isChanged(LatticeAccountProfile)) {
            log.info("Should run encode, because lattice account profile has changes");
            shouldRunEncode = true;
        }
        return shouldRunEncode;
    }

    private void expandEncAttrs(Table servingTable) {
        String profileAvroGlob = PathUtils.toAvroGlob(customerProfileTbl.toHdfsDataUnit("1").getPath());
        List<GenericRecord> records = new ArrayList<>(AvroUtils.getDataFromGlob(yarnConfiguration, profileAvroGlob));
        if (latticeProfileTbl != null) {
            profileAvroGlob = PathUtils.toAvroGlob(latticeProfileTbl.toHdfsDataUnit("1").getPath());
            records.addAll(AvroUtils.getDataFromGlob(yarnConfiguration, profileAvroGlob));
        }

        List<DCEncodedAttr> encAttrs = BucketEncodeUtils.encodedAttrs(records);
        Map<String, List<DCBucketedAttr>> bktAttrsMap = new HashMap<>();
        encAttrs.forEach(encAttr -> bktAttrsMap.put(encAttr.getEncAttr(), encAttr.getBktAttrs()));

        List<Attribute> attrs = new ArrayList<>();
        servingTable.getAttributes().forEach(attr -> {
            if (bktAttrsMap.containsKey(attr.getName())) {
                List<DCBucketedAttr> bktAttrs = bktAttrsMap.get(attr.getName());
                bktAttrs.forEach(bktAttr -> {
                    Attribute attribute = new Attribute();
                    attribute.setName(bktAttr.getNominalAttr());
                    attribute.setPhysicalName(attr.getName());
                    attribute.setBitOffset(bktAttr.getLowestBit());
                    attribute.setNumOfBits(bktAttr.getNumBits());
                    attribute.setPhysicalDataType(Schema.Type.STRING.getName());
                    attribute.setNullable(Boolean.TRUE);
                    attribute.setGroupsViaList(attr.getGroupsAsList());
                    attrs.add(attribute);
                });
            } else {
                attrs.add(attr);
            }
        });
        servingTable.setAttributes(attrs);
    }

    private void enrichTableSchema(Table servingTable) {
        String dataCloudVersion = configuration.getDataCloudVersion();
        ColumnMetadata latticeIdCm = columnMetadataProxy
                .columnSelection(ColumnSelection.Predefined.ID, dataCloudVersion).get(0);
        Map<String, Attribute> masterAttrs = new HashMap<>();
        customerAccountTbl.getAttributes().forEach(attr -> {
            masterAttrs.put(attr.getName(), attr);
        });
        Map<String, ColumnMetadata> amColMap = new HashMap<>();
        if (latticeAccountTbl != null) {
            latticeAccountTbl.getAttributes().forEach(attr -> {
                masterAttrs.put(attr.getName(), attr);
            });
            List<ColumnMetadata> amCols = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Segment,
                    dataCloudVersion);
            amCols.forEach(cm -> amColMap.put(cm.getAttrName(), cm));
        }

        List<Attribute> attrs = new ArrayList<>();
        final AtomicLong dcCount = new AtomicLong(0);
        final AtomicLong masterCount = new AtomicLong(0);
        servingTable.getAttributes().forEach(attr0 -> {
            Attribute attr = attr0;
            if (InterfaceName.LatticeAccountId.name().equals(attr0.getName())) {
                setupLatticeAccountIdAttr(latticeIdCm, attr);
                dcCount.incrementAndGet();
            } else if (amColMap.containsKey(attr0.getName())) {
                setupAmColMapAttr(amColMap, attr);
                dcCount.incrementAndGet();
            } else if (masterAttrs.containsKey(attr0.getName())) {
                attr = copyMasterAttr(masterAttrs, attr0);
                if (LogicalDataType.Date.equals(attr0.getLogicalDataType())) {
                    log.info("Setting last data refresh for profile date attribute: " + attr.getName() + " to "
                            + evaluationDateStr);
                    attr.setLastDataRefresh("Last Data Refresh: " + evaluationDateStr);
                }
                masterCount.incrementAndGet();
            }
            if (StringUtils.isBlank(attr.getCategory())) {
                attr.setCategory(Category.ACCOUNT_ATTRIBUTES);
            }
            if (Category.ACCOUNT_ATTRIBUTES.name().equals(attr.getCategory())) {
                attr.setSubcategory(null);
            }
            attr.removeAllowedDisplayNames();
            attrs.add(attr);
        });
        servingTable.setAttributes(attrs);
        log.info("Enriched " + dcCount.get() + " attributes using data cloud metadata.");
        log.info("Copied " + masterCount.get() + " attributes from batch store metadata.");
        log.info("BucketedAccount table has " + servingTable.getAttributes().size() + " attributes in total.");
    }

    private void enrichCustomerAccountTableSchema(Table table) {
        log.info("Attempt to enrich master table schema: " + table.getName());
        final List<Attribute> attrs = new ArrayList<>();
        final String evaluationDateStr = findEvaluationDate();
        final String ldrFieldValue = //
                StringUtils.isNotBlank(evaluationDateStr) ? ("Last Data Refresh: " + evaluationDateStr) : null;
        final AtomicLong updatedAttrs = new AtomicLong(0);
        table.getAttributes().forEach(attr0 -> {
            boolean updated = false;
            if (!attr0.hasTag(Tag.INTERNAL)) {
                attr0.setTags(Tag.INTERNAL);
                updated = true;
            }
            if (StringUtils.isNotBlank(ldrFieldValue) && LogicalDataType.Date.equals(attr0.getLogicalDataType())) {
                if (attr0.getLastDataRefresh() == null || !attr0.getLastDataRefresh().equals(ldrFieldValue)) {
                    log.info("Setting last data refresh for profile date attribute: " + attr0.getName() + " to "
                            + evaluationDateStr);
                    attr0.setLastDataRefresh(ldrFieldValue);
                    updated = true;
                }
            }
            if (updated) {
                updatedAttrs.incrementAndGet();
            }
            attrs.add(attr0);
        });
        if (updatedAttrs.get() > 0) {
            log.info("Found " + updatedAttrs.get() + " attrs to update, refresh master table schema.");
            table.setAttributes(attrs);
            String customerSpaceStr = customerSpace.toString();
            TableRoleInCollection batchStoreRole = BusinessEntity.Account.getBatchStore();
            String inactiveLink = dataCollectionProxy.getTableName(customerSpaceStr, batchStoreRole, inactive);
            String activeLink = dataCollectionProxy.getTableName(customerSpaceStr, batchStoreRole, active);
            metadataProxy.updateTable(customerSpaceStr, table.getName(), table);
            if (StringUtils.isNotBlank(inactiveLink) && inactiveLink.equalsIgnoreCase(table.getName())) {
                dataCollectionProxy.upsertTable(customerSpaceStr, inactiveLink, batchStoreRole, inactive);
            }
            if (StringUtils.isNotBlank(activeLink) && activeLink.equalsIgnoreCase(table.getName())) {
                dataCollectionProxy.upsertTable(customerSpaceStr, activeLink, batchStoreRole, active);
            }
        }
    }

    private void setupLatticeAccountIdAttr(ColumnMetadata latticeIdCm, Attribute attr) {
        attr.setInterfaceName(InterfaceName.LatticeAccountId);
        attr.setDisplayName(latticeIdCm.getDisplayName());
        attr.setDescription(latticeIdCm.getDescription());
        attr.setFundamentalType(FundamentalType.NUMERIC);
        attr.setCategory(latticeIdCm.getCategory());
        attr.setGroupsViaList(latticeIdCm.getEnabledGroups());
    }

    private void setupAmColMapAttr(Map<String, ColumnMetadata> amColMap, Attribute attr) {
        ColumnMetadata cm = amColMap.get(attr.getName());
        attr.setDisplayName(removeNonAscII(cm.getDisplayName()));
        attr.setDescription(removeNonAscII(cm.getDescription()));
        attr.setSubcategory(removeNonAscII(cm.getSubcategory()));
        attr.setFundamentalType(cm.getFundamentalType());
        attr.setCategory(cm.getCategory());
        attr.setGroupsViaList(cm.getEnabledGroups());
    }

    private String removeNonAscII(String str) {
        if (StringUtils.isNotBlank(str)) {
            return str.replaceAll("\\P{Print}", "");
        } else {
            return str;
        }
    }

}
