package com.latticeengines.pls.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelDetail;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.ModelDetailService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modeldetail", description = "REST resource for model summaries")
@RestController
@RequestMapping("/modeldetails")
@PreAuthorize("hasRole('View_PLS_Models')")
public class ModelDetailResource {

    @Autowired
    private ModelDetailService modelDetailService;
    @RequestMapping(value = "/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get detail for specific model")
    public ModelDetail getModelDetail(@PathVariable String modelId, HttpServletRequest request,
            HttpServletResponse response) {
        Tenant tenant = MultiTenantContext.getTenant();
        ModelDetail modelDetail = modelDetailService.getModelDetail(modelId);

        if (modelDetail == null) {
            throw new LedpException(LedpCode.LEDP_18124, new String[] { modelId, tenant.getId() });
        }

        return modelDetail;
    }
}
