package com.latticeengines.apps.lp.controller;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.lp.service.ModelCopyService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceapps.lp.CopyModelRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelcopy", description = "REST resource for model copy")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/modelcopy")
public class ModelCopyResource {

    @Inject
    private ModelCopyService modelCopyService;

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Copy model")
    public String copyModel(@PathVariable String customerSpace, @RequestBody CopyModelRequest request) {
        String sourceTenantId = MultiTenantContext.getTenantId();
        String targetTenantId = request.getTargetTenant();
        if (StringUtils.isBlank(targetTenantId)) {
            targetTenantId = sourceTenantId;
        }
        return modelCopyService.copyModel(sourceTenantId, targetTenantId, request.getModelGuid());
    }

    @PostMapping("/modelid/{modelId}/training-table")
    @ResponseBody
    @ApiOperation(value = "Clone training table")
    public Table cloneTrainingTable(@PathVariable String customerSpace, @PathVariable String modelSummaryId) {
        return modelCopyService.cloneTrainingTable(modelSummaryId);
    }

}
