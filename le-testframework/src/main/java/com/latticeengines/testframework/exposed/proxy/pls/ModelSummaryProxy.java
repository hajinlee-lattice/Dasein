package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.List;

import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.ModelSummary;

@Service("modelSummaryProxy")
public class ModelSummaryProxy extends PlsRestApiProxyBase {

    public ModelSummaryProxy() {
        super("pls/modelsummaries");
    }

    public List<ModelSummary> getSummaries() {
        String url = constructUrl("/");
        List<?> list = get("get model summaries", url, List.class);
        return JsonUtils.convertList(list, ModelSummary.class);
    }

    public ModelSummary getModelSummary(String modelId) {
        String url = constructUrl("/{modelId}/", modelId);
        return get("get model summary", url, ModelSummary.class);
    }

    public ModelSummary createModelSummary(ModelSummary modelSummary) {
        String url = constructUrl("/");
        return post("create model summary", url, modelSummary, ModelSummary.class);
    }

}
