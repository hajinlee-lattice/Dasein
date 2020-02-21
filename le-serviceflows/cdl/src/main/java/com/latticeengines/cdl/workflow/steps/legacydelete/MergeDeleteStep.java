package com.latticeengines.cdl.workflow.steps.legacydelete;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.LegacyDeleteByUploadActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.MergeImportsJob;

@Component(MergeDeleteStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class MergeDeleteStep extends RunSparkJob<LegacyDeleteSparkStepConfiguration, MergeImportsConfig> {

    private static final Logger log = LoggerFactory.getLogger(MergeDeleteStep.class);

    static final String BEAN_NAME = "mergeDeleteStep";

    @Override
    protected Class<? extends AbstractSparkJob<MergeImportsConfig>> getJobClz() {
        return MergeImportsJob.class;
    }

    @Override
    protected MergeImportsConfig configureJob(LegacyDeleteSparkStepConfiguration stepConfiguration) {
        Set<Action> inputs = initialData(stepConfiguration);
        log.info("input is {}. stepConfig is {}.", inputs, JsonUtils.serialize(stepConfiguration));
        if (CollectionUtils.isEmpty(inputs)) {
            return null;
        }
        MergeImportsConfig mergeImportsConfig = MergeImportsConfig.joinBy(getJoinKey(configuration.getEntity()));
        mergeImportsConfig.setDedupSrc(true);
        mergeImportsConfig.setAddTimestamps(false);
        List<DataUnit> units = new ArrayList<>();
        inputs.forEach(action -> {
                LegacyDeleteByUploadActionConfiguration configuration = (LegacyDeleteByUploadActionConfiguration) action.getActionConfiguration();
                Table table = metadataProxy.getTable(stepConfiguration.getCustomer(),
                        configuration.getTableName());
                units.add(table.toHdfsDataUnit(configuration.getTableName()));
            });
        mergeImportsConfig.setInput(units);
        return mergeImportsConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String tenantId = CustomerSpace.shortenCustomerSpace(parseCustomerSpace(configuration).toString());
        String mergedTableName = NamingUtils.timestamp("DeleteFile_");
        Table mergedTable = toTable(mergedTableName, configuration.getEntity().getServingStore().getPrimaryKey().name(),
                result.getTargets().get(0));
        metadataProxy.createTable(tenantId, mergedTableName, mergedTable);
        Map<BusinessEntity, Table> mergeTables = getMapObjectFromContext(LEGACY_DELETE_MERGE_TABLENAMES,
                BusinessEntity.class, Table.class);
        if (mergeTables == null) {
            mergeTables = new HashMap<>();
        }
        mergeTables.put(configuration.getEntity(), mergedTable);
        putObjectInContext(LEGACY_DELETE_MERGE_TABLENAMES, mergeTables);
    }

    private String getJoinKey(BusinessEntity entity) {
        switch (entity) {
            case Account:
                return InterfaceName.AccountId.name();
            case Contact:
                return InterfaceName.ContactId.name();
            default:
                return null;
        }
    }

    private Set<Action> initialData(LegacyDeleteSparkStepConfiguration stepConfiguration) {
        switch (stepConfiguration.getEntity()) {
            case Account:
                return getSetObjectFromContext(ACCOUNT_LEGACY_DELTE_BYUOLOAD_ACTIONS, Action.class);
            case Contact:
                return getSetObjectFromContext(CONTACT_LEGACY_DELTE_BYUOLOAD_ACTIONS, Action.class);
            default:
                return null;
        }
    }

}
