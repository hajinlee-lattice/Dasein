package com.latticeengines.cdl.workflow.steps.maintenance;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.CleanupAllStepConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("cleanupAllStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CleanupAllStep extends BaseWorkflowStep<CleanupAllStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CleanupAllStep.class);

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${camille.zk.pod.id:Default}")
    protected String podId;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Override
    public void execute() {//just clean up all data for entity not include metadata and attribute at inactive version
        CleanupAllStepConfiguration cleanupAllStepConfiguration = getConfiguration();
        if (CollectionUtils.isNotEmpty(cleanupAllStepConfiguration.getEntitySet())) {
            log.info("need deleted Entity set: " + JsonUtils.serialize(cleanupAllStepConfiguration.getEntitySet()));
            for (BusinessEntity entity : cleanupAllStepConfiguration.getEntitySet()) {
                deleteData(entity, cleanupAllStepConfiguration.getCustomerSpace().toString());
            }
        }
    }

    private void deleteData(BusinessEntity entity, String customerSpace) {
        log.info(String.format("begin clean up cdl data of CustomerSpace %s", customerSpace));
        DataCollection.Version inactiveVersion = dataCollectionProxy.getInactiveVersion(customerSpace);
        if (entity == BusinessEntity.Transaction) {
            cleanupRedshift(customerSpace,
                    Arrays.asList(TableRoleInCollection.ConsolidatedRawTransaction,
                            TableRoleInCollection.ConsolidatedDailyTransaction,
                            TableRoleInCollection.ConsolidatedPeriodTransaction),
                    inactiveVersion);
            cleanupS3(customerSpace,
                    Arrays.asList(TableRoleInCollection.ConsolidatedRawTransaction,
                            TableRoleInCollection.ConsolidatedDailyTransaction,
                            TableRoleInCollection.ConsolidatedPeriodTransaction),
                    inactiveVersion);
            dataCollectionProxy.resetTable(customerSpace, TableRoleInCollection.ConsolidatedRawTransaction);
            dataCollectionProxy.resetTable(customerSpace,
                    TableRoleInCollection.ConsolidatedDailyTransaction);
            dataCollectionProxy.resetTable(customerSpace,
                    TableRoleInCollection.ConsolidatedPeriodTransaction);
            dataFeedProxy.resetImportByEntity(customerSpace, entity.name());
        } else if (entity == BusinessEntity.Account || entity == BusinessEntity.Contact
                || entity == BusinessEntity.Product) {
            cleanupRedshift(customerSpace, Arrays.asList(entity.getBatchStore()),
                    inactiveVersion);
            cleanupS3(customerSpace, Arrays.asList(entity.getBatchStore()),
                    inactiveVersion);
            dataCollectionProxy.resetTable(customerSpace, entity.getBatchStore());
            dataFeedProxy.resetImportByEntity(customerSpace, entity.name());
        } else {
            log.info(String.format("current Business entity is %s;", entity.name()));
            throw new RuntimeException(String.format("current Business entity is %s, unsupported", entity.name()));
        }

    }

    private void cleanupRedshift(String customSpace, List<TableRoleInCollection> roles,
                                 DataCollection.Version version) {
        try {
            roles.forEach(role -> {
                Table table = dataCollectionProxy.getTable(customSpace, role, version);
                if (table != null) {
                    List<String> redshiftTables = redshiftService.getTables(table.getName());
                    if (CollectionUtils.isNotEmpty(redshiftTables)) {
                        redshiftTables.forEach(redshiftTable -> redshiftService.dropTable(redshiftTable));
                    }
                }
            });
        } catch (Exception e) {
            log.error(String.format("Cannot cleanup redshift tables for %s", customSpace));
        }
    }

    private void cleanupS3(String customSpace, List<TableRoleInCollection> roles,
                           DataCollection.Version version) {
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
        try {
            roles.forEach(role -> {
                Table table = dataCollectionProxy.getTable(customSpace, role, version);
                if (table != null) {
                    List<Extract> extracts = table.getExtracts();
                    if (CollectionUtils.isEmpty(extracts) || StringUtils.isBlank(extracts.get(0).getPath())) {
                        log.warn("Can not find extracts of the table=" + table.getName() + " for tenant=" + customSpace);
                        return;
                    }
                    String srcDir = pathBuilder.getFullPath(extracts.get(0).getPath());
                    String tenantId = CustomerSpace.parse(customSpace).getTenantId();
                    String tgtDir = pathBuilder.convertAtlasTableDir(srcDir, podId, tenantId, s3Bucket);
                    String prefix = tgtDir.substring(tgtDir.indexOf(s3Bucket) + s3Bucket.length() + 1);
                    s3Service.cleanupPrefix(s3Bucket, prefix);
                } else {
                    log.warn("Cannot find table for table role: " + role.name());
                }
            });
        } catch (Exception e) {
            log.error(String.format("Cannot cleanup s3 tables for %s", customSpace));
        }
    }
}
