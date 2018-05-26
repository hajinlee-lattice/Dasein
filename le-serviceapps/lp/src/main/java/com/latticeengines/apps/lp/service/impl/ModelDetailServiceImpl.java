package com.latticeengines.apps.lp.service.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.apps.lp.service.ModelDetailService;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.CategoryObject;
import com.latticeengines.domain.exposed.pls.ModelDetail;
import com.latticeengines.domain.exposed.pls.ModelSummary;

@Component("modelDetailService")
public class ModelDetailServiceImpl implements ModelDetailService {
    private static final String MODEL_SUMMARY_PREDICTORS = "Predictors";
    private static final String MODEL_SUMMARY_CATEGORY = "Category";
    private static final String MODEL_SUMMARY_TAGS = "Tags";
    private static final String INTERNAL = "Internal";
    private static final char FROM_DELIMETER = ' ';
    private static final char TO_DELIMETER = '_';

    @Inject
    private ModelSummaryService modelSummaryService;

    @Override
    public ModelDetail getModelDetail(String modelId) {
        ModelSummary modelSummary = modelSummaryService.getModelSummary(modelId);
        ModelDetail modelDetail = new ModelDetail();
        modelDetail.setModelSummary(modelSummary);
        Map<String, CategoryObject> map = new HashMap<>();
        JsonNode node = JsonUtils.deserialize(modelSummary.getDetails().getPayload(), JsonNode.class);
        JsonNode predictorsJsonNode = node.get(MODEL_SUMMARY_PREDICTORS);
        if (!predictorsJsonNode.isArray()) {
            throw new IllegalArgumentException("The modelsummary should be a JSON Array.");
        }
        Set<String> uniqueSet = new HashSet<>();
        for (final JsonNode predictorJson : predictorsJsonNode) {
            String displayName = JsonUtils.getOrDefault(predictorJson.get(MODEL_SUMMARY_CATEGORY), String.class, "");
            if (!StringUtils.isEmpty(displayName) && !uniqueSet.contains(displayName)) {
                uniqueSet.add(displayName);
            } else {
                continue;
            }
            CategoryObject object = new CategoryObject();
            object.setDisplayName(displayName);
            String tagsString = JsonUtils.getOrDefault(predictorJson.get(MODEL_SUMMARY_TAGS), String.class, "");
            if(tagsString.contains(INTERNAL)) {
                object.setIsInternal(true);
            } else {
                object.setIsInternal(false);
            }
            map.put(displayName.replace(FROM_DELIMETER, TO_DELIMETER), object);
        }
        modelDetail.setCategories(map);
        return modelDetail;
    }

}
