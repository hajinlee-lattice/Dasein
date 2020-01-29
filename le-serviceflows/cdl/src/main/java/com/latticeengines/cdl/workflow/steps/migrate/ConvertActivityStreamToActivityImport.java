package com.latticeengines.cdl.workflow.steps.migrate;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.maintenance.SoftDeleteActivityStream;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;

@Component(ConvertActivityStreamToActivityImport.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConvertActivityStreamToActivityImport extends SoftDeleteActivityStream {

    static final String BEAN_NAME = "convertActivityStreamToActivityImport";

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        Map<String, String> rawStreamTables = buildRawStreamBatchStore();
        updateEntityValueMapInContext(PERFORM_HARD_DELETE, Boolean.TRUE, Boolean.class);
        if (MapUtils.isNotEmpty(rawStreamTables)) {
            Map<String, List<ActivityImport>> streamImports = new HashMap<>();
            Map<String, AtlasStream> streamMap = configuration.getActivityStreamMap();
            if (MapUtils.isNotEmpty(streamMap)) {
                rawStreamTables.forEach((streamId, tableName) -> {
                    AtlasStream atlasStream = streamMap.get(streamId);
                    if (atlasStream != null) {
                        streamImports.put(streamId,
                                Collections.singletonList(new ActivityImport(BusinessEntity.getByName(atlasStream.getDataFeedTask().getEntity()),
                                        atlasStream.getStreamId(), tableName, "HardDeleteFile")));
                    }
                });
            }
            putObjectInContext(ACTIVITY_IMPORT_AFTER_HARD_DELETE, streamImports);
        }
    }
}
