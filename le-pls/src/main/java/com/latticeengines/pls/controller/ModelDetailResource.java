package com.latticeengines.pls.controller;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelDetail;
import com.latticeengines.proxy.exposed.lp.ModelDetailProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modeldetail", description = "REST resource for model summaries")
@RestController
@RequestMapping("/modeldetails")
@PreAuthorize("hasRole('View_PLS_Models')")
public class ModelDetailResource {

    @Inject
    private ModelDetailProxy modelDetailProxy;

    @GetMapping("/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get detail for specific model")
    public ModelDetail getModelDetail(@PathVariable String modelId) {
        String tenantId = MultiTenantContext.getShortTenantId();
        ModelDetail modelDetail = modelDetailProxy.getModelDetail(tenantId, modelId);
        if (modelDetail == null) {
            throw new LedpException(LedpCode.LEDP_18124, new String[] { modelId, tenantId });
        }
        return modelDetail;
    }
}
