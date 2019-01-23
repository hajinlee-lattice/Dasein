package com.latticeengines.apps.lp.controller;

import java.util.concurrent.ExecutorService;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.lp.service.ModelCopyService;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.lp.CopyModelRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelcopy", description = "REST resource for model copy")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/modelcopy")
public class ModelCopyResource {

    private static final Logger log = LoggerFactory.getLogger(ModelCopyResource.class);

    @Inject
    private ModelCopyService modelCopyService;

    private ExecutorService workers = null;

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Copy model")
    public String copyModel(@PathVariable String customerSpace, @RequestBody CopyModelRequest request) {

        String sourceTenantId = MultiTenantContext.getShortTenantId();
        String targetTenantId = request.getTargetTenant();
        if (StringUtils.isBlank(targetTenantId)) {
            targetTenantId = sourceTenantId;
        }
        final String finalTargetTenantId = targetTenantId;
        final Tenant sourceTenant = MultiTenantContext.getTenant();
        if ("true".equals(request.getAsync())) {
            log.info("Starting Aync model copy.");
            getWorkers().submit(() -> {
                try {
                    MultiTenantContext.setTenant(sourceTenant);
                    String newModelGuid = modelCopyService.copyModel(sourceTenantId, finalTargetTenantId,
                            request.getModelGuid());
                    log.info(String.format(
                            "Finished model copy, source model Id=%s, new model Id=%s, source tenant=%s, target tenant=%s",
                            request.getModelGuid(), newModelGuid, sourceTenantId, finalTargetTenantId));
                } catch (Exception ex) {
                    log.error("Failed to copy model in async mode!", ex);
                }
            });
            return "";
        } else {
            return modelCopyService.copyModel(sourceTenantId, finalTargetTenantId, request.getModelGuid());
        }
    }

    @PostMapping("/modelid/{modelId}/training-table")
    @ResponseBody
    @ApiOperation(value = "Clone training table")
    public Table cloneTrainingTable(@PathVariable String customerSpace, @PathVariable String modelId) {
        return modelCopyService.cloneTrainingTable(modelId);
    }

    private ExecutorService getWorkers() {
        if (workers == null) {
            synchronized (this) {
                if (workers == null) {
                    workers = ThreadPoolUtils.getCachedThreadPool("modelCopy");
                }
            }
        }
        return workers;
    }
}
