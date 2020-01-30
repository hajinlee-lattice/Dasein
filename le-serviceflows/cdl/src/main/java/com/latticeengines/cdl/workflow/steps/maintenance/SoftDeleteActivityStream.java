package com.latticeengines.cdl.workflow.steps.maintenance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;

@Component(SoftDeleteActivityStream.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SoftDeleteActivityStream extends BaseDeleteActivityStream<ProcessActivityStreamStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SoftDeleteActivityStream.class);

    static final String BEAN_NAME = "softDeleteActivityStream";

    List<Action> softDeleteActions;


    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        if (configuration.isRematchMode()) {
            return;
        }
        Map<String, String> rawStreamTables = buildRawStreamBatchStore();
        exportToS3AndAddToContext(rawStreamTables, RAW_ACTIVITY_STREAM_TABLE_NAME);
        updateEntityValueMapInContext(PERFORM_SOFT_DELETE, Boolean.TRUE, Boolean.class);
        if (MapUtils.isNotEmpty(rawStreamTables)) {
            putObjectInContext(RAW_STREAM_TABLE_AFTER_DELETE, rawStreamTables);
        }
    }

    @Override
    protected List<Action> deleteActions() {
        return softDeleteActions;
    }

    @Override
    protected String getBeanName() {
        return BEAN_NAME;
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        softDeleteActions = getListObjectFromContext(SOFT_DEELETE_ACTIONS, Action.class);
    }

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {
        if (configuration.isRematchMode()) {
            log.info("Activity Stream use replace mode. Skip soft delete!");
            return null;
        }
        return super.getConsolidateRequest();
    }

    protected TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getConsolidateRequest();
        if (request == null) {
            return null;
        } else {
            return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
        }
    }

    protected <V> void updateEntityValueMapInContext(String key, V value, Class<V> clz) {
        updateEntityValueMapInContext(entity, key, value, clz);
    }

    protected <V> void updateEntityValueMapInContext(BusinessEntity entity, String key, V value, Class<V> clz) {
        Map<BusinessEntity, V> entityValueMap = getMapObjectFromContext(key, BusinessEntity.class, clz);
        if (entityValueMap == null) {
            entityValueMap = new HashMap<>();
        }
        entityValueMap.put(entity, value);
        putObjectInContext(key, entityValueMap);
    }
}
