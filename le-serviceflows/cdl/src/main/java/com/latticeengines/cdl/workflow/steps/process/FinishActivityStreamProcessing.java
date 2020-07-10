package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.admin.LatticeFeatureFlag.ENABLE_ACCOUNT360;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component(FinishActivityStreamProcessing.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class FinishActivityStreamProcessing extends BaseWorkflowStep<ProcessActivityStreamStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(FinishActivityStreamProcessing.class);
    private static final String PARTITION_KEY_NAME = InterfaceName.PartitionKey.name();
    private static final String SORT_KEY_NAME = InterfaceName.SortKey.name();

    static final String BEAN_NAME = "finishActivityStreamProcessing";

    @Inject
    private BatonService batonService;

    @Value("${cdl.processAnalyze.skip.dynamo.publication}")
    private boolean skipPublishDynamo;

    @Override
    public void execute() {
        publishTimelineDiffTablesToDynamo();
    }

    public void publishTimelineDiffTablesToDynamo() {
        if (shouldPublishTimelineToDynamo()) {
            log.info("Skip publishing timeline diff table to dynamo. Account360 enabled = {}, skipPublishDynamo = {}", account360Enabled(), skipPublishDynamo);
            return;
        }

        Map<String, String> timelineTableNames = getMapObjectFromContext(TIMELINE_DIFF_TABLE_NAME, String.class, String.class);
        if (MapUtils.isEmpty(timelineTableNames)) {
            log.info("No timeline diff table found in context, skip publishing to dynamo");
            return;
        }

        log.info("Publishing timeline diff tables {} to dynamo", timelineTableNames);
        timelineTableNames.values().forEach(tableName -> {
            exportToDynamo(tableName);
            addToListInContext(TEMPORARY_CDL_TABLES, tableName, String.class);
        });
    }

    private void exportToDynamo(String tableName) {
        String inputPath = metadataProxy.getAvroDir(configuration.getCustomerSpace().toString(), tableName);
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(tableName);
        config.setInputPath(PathUtils.toAvroGlob(inputPath));
        config.setPartitionKey(PARTITION_KEY_NAME);
        config.setSortKey(SORT_KEY_NAME);
        addToListInContext(TIMELINE_RAWTABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
    }

    private boolean account360Enabled() {
        return batonService.isEnabled(configuration.getCustomerSpace(), ENABLE_ACCOUNT360);
    }

    private boolean shouldPublishTimelineToDynamo() {
        return !skipPublishDynamo && account360Enabled();
    }
}
