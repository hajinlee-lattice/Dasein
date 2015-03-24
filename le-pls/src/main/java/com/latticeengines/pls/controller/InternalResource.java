package com.latticeengines.pls.controller;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.Status;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelActivationResult;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ResponseDocument;
import com.latticeengines.domain.exposed.pls.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.GrantedRight;
import com.latticeengines.pls.security.RightsUtilities;
import com.latticeengines.pls.service.TenantService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "internal", description = "REST resource for internal operations")
@RestController
@RequestMapping(value = "/internal")
public class InternalResource extends InternalResourceBase {

    private static final Log log = LogFactory.getLog(InternalResource.class);

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private TenantService tenantService;

    @RequestMapping(value = "/modelsummaries/{modelId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a model summary")
    public ResponseDocument<ModelActivationResult> update(@PathVariable String modelId,
            @RequestBody AttributeMap attrMap, HttpServletRequest request) {
        checkHeader(request);
        ModelSummary summary = modelSummaryEntityMgr.getByModelId(modelId);

        if (summary == null) {
            ModelActivationResult result = new ModelActivationResult();
            result.setExists(false);
            ResponseDocument<ModelActivationResult> response = new ResponseDocument<>();
            response.setSuccess(false);
            response.setResult(result);
            return response;
        }

        ((ModelSummaryEntityMgrImpl) modelSummaryEntityMgr).manufactureSecurityContextForInternalAccess(summary
                .getTenant());

        // Reuse the logic in the ModelSummaryResource to do the updates
        ModelSummaryResource msr = new ModelSummaryResource();
        msr.setModelSummaryEntityMgr(modelSummaryEntityMgr);
        ModelActivationResult result = new ModelActivationResult();
        result.setExists(true);
        ResponseDocument<ModelActivationResult> response = new ResponseDocument<>();
        response.setResult(result);
        if (msr.update(modelId, attrMap)) {
            response.setSuccess(true);
        } else {
            response.setSuccess(false);
        }
        return response;
    }

    @RequestMapping(value = "/users", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete users. Mainly for cleaning up testing users.")
    public SimpleBooleanResponse deleteUsers(
        @RequestParam(value="namepattern", required=false) String namePattern,
        @RequestParam(value="tenant", required=false) String tenantId,
        HttpServletRequest request
    ) throws URIException {
        checkHeader(request);

        if (namePattern != null) {
            namePattern = URIUtil.decode(namePattern);
            List<String> tenants = new ArrayList<>();
            if (tenantId == null) {
                log.info(String.format("Deleting users matching %s from all tenants using internal API", namePattern));
                for (Tenant t: tenantService.getAllTenants()){
                    tenants.add(t.getId());
                }
                // return SimpleBooleanResponse.getFailResponse(Arrays.asList("Must specify tenant when deleting users by a name pattern."));
            } else {
                tenants.add(tenantId);
                log.info(String.format("Deleting users matching %s from the tenant %s using internal API", namePattern, tenantId));
            }

            for (String tid: tenants) {
                List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList = globalUserManagementService.getAllUsersOfTenant(tid);
                for (AbstractMap.SimpleEntry<User, List<String>> userRight: userRightsList) {
                    User user = userRight.getKey();
                    boolean isAdmin = RightsUtilities.isAdmin(
                        RightsUtilities.translateRights(userRight.getValue())
                    );
                    if ((!isAdmin) && user.getUsername().matches(namePattern)) {
                        softDelete(tid, user.getUsername());
                        log.info(String.format("User %s has been soft deleted from the tenant %s through internal API", user.getUsername(), tid));
                        if (globalUserManagementService.isRedundant(user.getUsername())) {
                            globalUserManagementService.deleteUser(user.getUsername());
                            log.info(String.format("User %s has been hard deleted through internal API", user.getUsername()));
                        }
                    }
                }
            }
        } else {
            log.info(String.format("Username pattern not found"));
        }

        return SimpleBooleanResponse.getSuccessResponse();
    }

    @RequestMapping(value = "/{op}/{left}/{right}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Status check for this endpoint")
    public Status calculate(@PathVariable("op") String op, @PathVariable("left") Integer left,
            @PathVariable("right") Integer right) {
        Assert.notNull(op);
        Assert.notNull(left);
        Assert.notNull(right);
        Status result = new Status();
        result.setOperation(op);
        result.setLeft(left);
        result.setRight(right);
        return doCalc(result);
    }

    private Status doCalc(Status c) {
        String op = c.getOperation();
        int left = c.getLeft();
        int right = c.getRight();
        if (op.equalsIgnoreCase("subtract")) {
            c.setResult(left - right);
        } else if (op.equalsIgnoreCase("multiply")) {
            c.setResult(left * right);
        } else if (op.equalsIgnoreCase("divide")) {
            c.setResult(left / right);
        } else {
            c.setResult(left + right);
        }
        return c;
    }

    private void softDelete(String tenantId, String username) {
        revokeRightWithoutException(GrantedRight.VIEW_PLS_MODELS.getAuthority(), tenantId, username);
        revokeRightWithoutException(GrantedRight.VIEW_PLS_CONFIGURATION.getAuthority(), tenantId, username);
        revokeRightWithoutException(GrantedRight.VIEW_PLS_USERS.getAuthority(), tenantId, username);
        revokeRightWithoutException(GrantedRight.VIEW_PLS_REPORTING.getAuthority(), tenantId, username);
        revokeRightWithoutException(GrantedRight.EDIT_PLS_MODELS.getAuthority(), tenantId, username);
        revokeRightWithoutException(GrantedRight.EDIT_PLS_CONFIGURATION.getAuthority(), tenantId, username);
        revokeRightWithoutException(GrantedRight.EDIT_PLS_USERS.getAuthority(), tenantId, username);
        revokeRightWithoutException(GrantedRight.CREATE_PLS_MODELS.getAuthority(), tenantId, username);
    }

    private void revokeRightWithoutException(String right, String tenant, String username) {
        try {
            globalUserManagementService.revokeRight(right, tenant, username);
        } catch (Exception e) {
            // ignore
        }
    }

}
