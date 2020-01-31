package com.latticeengines.cdl.workflow.steps.migrate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.maintenance.BaseDeleteActivityStream;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;

@Component(ConvertActivityStreamToActivityImport.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConvertActivityStreamToActivityImport extends BaseDeleteActivityStream<ProcessActivityStreamStepConfiguration> {

    static final String BEAN_NAME = "convertActivityStreamToActivityImport";

    List<Action> hardDeleteActions;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        hardDeleteActions = getListObjectFromContext(HARD_DEELETE_ACTIONS, Action.class);
    }

    @Override
    protected void onPostTransformationCompleted() {
        Map<String, String> rawStreamTables = buildRawStreamBatchStore();
        updateEntityValueMapInContext(PERFORM_HARD_DELETE, Boolean.TRUE, Boolean.class);
        if (MapUtils.isNotEmpty(rawStreamTables)) {
            Map<String, ActivityImport> streamImports = new HashMap<>();
            Map<String, AtlasStream> streamMap = configuration.getActivityStreamMap();
            if (MapUtils.isNotEmpty(streamMap)) {
                rawStreamTables.forEach((streamId, tableName) -> {
                    AtlasStream atlasStream = streamMap.get(streamId);
                    if (atlasStream != null) {
                        streamImports.put(streamId,
                                new ActivityImport(getEntityFromStream(atlasStream),
                                        atlasStream.getStreamId(), tableName, "HardDeleteFile"));
                    }
                });
            }
            putObjectInContext(ACTIVITY_IMPORT_AFTER_HARD_DELETE, streamImports);
        }
    }

    private BusinessEntity getEntityFromStream(AtlasStream atlasStream) {
        if (atlasStream.getDataFeedTask() == null) {
            return null;
        }
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(),
                atlasStream.getDataFeedTaskUniqueId());
        if (dataFeedTask != null && StringUtils.isNotEmpty(dataFeedTask.getEntity())) {
            return BusinessEntity.getByName(dataFeedTask.getEntity());
        }
        return null;
    }

    @Override
    protected List<Action> deleteActions() {
        return hardDeleteActions;
    }

    @Override
    protected String getBeanName() {
        return BEAN_NAME;
    }

    @Override
    protected Boolean needPartition() {
        return Boolean.FALSE;
    }
}
