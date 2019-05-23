package com.latticeengines.cdl.workflow.steps.rebuild;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.common.CopyJob;


@Component(FilterAccountExport.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FilterAccountExport extends RunSparkJob<ProcessAccountStepConfiguration, CopyConfig, CopyJob> {

    private static final Logger log = LoggerFactory.getLogger(FilterAccountExport.class);

    static final String BEAN_NAME = "filterAccountExport";

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private boolean shortCutMode = false;
    private DataCollection.Version inactive;

    @Override
    protected Class<CopyJob> getJobClz() {
        return CopyJob.class;
    }

    @Override
    protected CopyConfig configureJob(ProcessAccountStepConfiguration stepConfiguration) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        String accountExpotTableName = getStringValueFromContext(ACCOUNT_EXPORT_TABLE_NAME);
        if (StringUtils.isNotBlank(accountExpotTableName)) {
            Table accountFeatureTable = metadataProxy.getTable(customerSpace.toString(), accountExpotTableName);
            if (accountFeatureTable != null) {
                log.info("Found account export table in context, going thru short-cut mode.");
                shortCutMode = true;
                dataCollectionProxy.upsertTable(customerSpace.toString(), accountExpotTableName, //
                        TableRoleInCollection.AccountExport, inactive);
                return null;
            }
        }

        String fullAccountTableName = getStringValueFromContext(FULL_ACCOUNT_TABLE_NAME);
        if (StringUtils.isBlank(fullAccountTableName)) {
            throw new IllegalStateException("Cannot find the fully enriched account table");
        }
        Table fullAccountTable = metadataProxy.getTable(customerSpace.toString(), fullAccountTableName);
        if (fullAccountTable == null) {
            throw new IllegalStateException("Cannot find the fully enriched account table in default collection");
        }

        CopyConfig config = new CopyConfig();
        config.setInput(Collections.singletonList(fullAccountTable.toHdfsDataUnit("FullAccount")));
        config.setSelectAttrs(getRetrainAttrNames());
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        if (shortCutMode) {
            return;
        }
        String filteredTableName = NamingUtils.timestamp("AccountExport");
        Table filteredTable = toTable(filteredTableName, InterfaceName.AccountId.name(), result.getTargets().get(0));
        metadataProxy.createTable(customerSpace.toString(), filteredTableName, filteredTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), filteredTableName, //
                TableRoleInCollection.AccountExport, inactive);
        exportToS3AndAddToContext(filteredTable, ACCOUNT_EXPORT_TABLE_NAME);
    }

    private List<String> getRetrainAttrNames() {
        List<String> retainAttrNames = servingStoreProxy
                .getDecoratedMetadata(customerSpace.toString(), BusinessEntity.Account, null,
                        inactive) //
                .filter(cm -> !AttrState.Inactive.equals(cm.getAttrState())) //
                .filter(cm -> !(Boolean.FALSE.equals(cm.getCanSegment()) //
                        && Boolean.FALSE.equals(cm.getCanEnrich()))) //
                .map(ColumnMetadata::getAttrName) //
                .collectList().block();
        if (retainAttrNames == null) {
            retainAttrNames = new ArrayList<>();
        }
        if (!retainAttrNames.contains(InterfaceName.AccountId.name())) {
            retainAttrNames.add(InterfaceName.AccountId.name());
        }
        return retainAttrNames;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(ProcessAccountStepConfiguration stepConfiguration) {
        return stepConfiguration.getCustomerSpace();
    }

}

