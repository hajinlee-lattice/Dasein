package com.latticeengines.datacloudapi.api.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloudapi.engine.orchestration.service.OrchestrationService;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.network.exposed.propdata.OrchestrationInterface;
import com.latticeengines.security.exposed.InternalResourceBase;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "orchestration", description = "REST resource for orchestrations")
@RestController
@RequestMapping("/orchestrations")
public class OrchestrationResource extends InternalResourceBase implements OrchestrationInterface {

    @Autowired
    private OrchestrationService orchestrationService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Scan and trigger all engine jobs that can proceed.")
    public List<OrchestrationProgress> scan(
            @RequestParam(value = "HdfsPod", required = false, defaultValue = "") String hdfsPod,
            HttpServletRequest request) {
        checkHeader(request);
        try {
            return orchestrationService.scan(hdfsPod);
        } catch (Exception e) {
            throw new RuntimeException("Fail to scan orchestrations", e);
        }
    }

    @Override
    public List<OrchestrationProgress> scan(String hdfsPod) {
        throw new UnsupportedOperationException("This is a place holder of a proxy method.");
    }
}
