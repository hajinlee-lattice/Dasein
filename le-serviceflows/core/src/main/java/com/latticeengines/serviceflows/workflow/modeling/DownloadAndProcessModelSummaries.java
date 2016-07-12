package com.latticeengines.serviceflows.workflow.modeling;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modelreview.BaseRuleResult;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

@Component("downloadAndProcessModelSummaries")
public class DownloadAndProcessModelSummaries extends BaseWorkflowStep<ModelStepConfiguration> {

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private WaitForDownloadedModelSummaries waitForDownloadedModelSummaries;

    private InternalResourceRestApiProxy proxy = null;

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
        if (proxy == null) {
            proxy = new InternalResourceRestApiProxy(configuration.getInternalResourceHostPort());
        }

        Map<String, String> modelApplicationIdToEventColumn = JsonUtils.deserialize(
                executionContext.getString(MODEL_APP_IDS), Map.class);
        if (modelApplicationIdToEventColumn == null || modelApplicationIdToEventColumn.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_28012);
        }
        Map<String, String> eventToModelId = waitForDownloadedModelSummaries.retrieveModelIds(configuration,
                modelApplicationIdToEventColumn);

        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", ModelSummaryStatus.INACTIVE.getStatusCode());
        for (String modelId : eventToModelId.values()) {
            proxy.updateModelSummary(modelId, attrMap);
        }

        persistReviewResults(eventToModelId);
    }

    @SuppressWarnings("unchecked")
    private void persistReviewResults(Map<String, String> eventToModelId) {
        Map<String, List<ColumnRuleResult>> eventToColumnResults = (Map<String, List<ColumnRuleResult>>) executionContext
                .get(COLUMN_RULE_RESULTS);
        Map<String, List<RowRuleResult>> eventToRowResults = (Map<String, List<RowRuleResult>>) executionContext
                .get(ROW_RULE_RESULTS);
        if (eventToColumnResults == null || eventToRowResults == null) {
            log.warn("COLUMN_RULE_RESULTS or ROW_RULE_RESULTS is null");
            return;
        }
        Tenant tenant = tenantEntityMgr.findByTenantId(configuration.getCustomerSpace().toString());
        for (String event : eventToModelId.keySet()) {
            String modelId = eventToModelId.get(event);

            List<ColumnRuleResult> columnResults = eventToColumnResults.get(event);
            List<RowRuleResult> rowResults = eventToRowResults.get(event);
            setModelIdAndTenantOnRuleResults(columnResults, modelId, tenant);
            setModelIdAndTenantOnRuleResults(rowResults, modelId, tenant);
            metadataProxy.createColumnResults(columnResults);
            metadataProxy.createRowResults(rowResults);
        }
    }

    private void setModelIdAndTenantOnRuleResults(List<? extends BaseRuleResult> results, String modelId, Tenant tenant) {
        for (BaseRuleResult result : results) {
            result.setModelId(modelId);
            result.setTenant(tenant);
        }
    }
}
