package com.latticeengines.pls.controller;

import javax.servlet.http.HttpServletRequest;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.Status;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl;
import com.latticeengines.pls.exception.LoginException;
import com.latticeengines.pls.globalauth.authentication.impl.Constants;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "internal", description = "REST resource for internal operations")
@RestController
@RequestMapping(value = "/internal")
public class InternalResource {
    
    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;
    
    @Autowired
    private SessionFactory sessionFactory;

    @RequestMapping(value = "/modelsummaries/{modelId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a model summary")
    public Boolean update(@PathVariable String modelId, @RequestBody AttributeMap attrMap, HttpServletRequest request) {
        String value = request.getHeader(Constants.INTERNAL_SERVICE_HEADERNAME);
        
        if (value == null || !value.equals(Constants.INTERNAL_SERVICE_HEADERVALUE)) {
            throw new LoginException(new LedpException(LedpCode.LEDP_18001, new String[] {}));
        }
        ModelSummary summary = modelSummaryEntityMgr.getByModelId(modelId);
        
        if (summary == null) {
            return false;
        }
    
        ((ModelSummaryEntityMgrImpl) modelSummaryEntityMgr).manufactureSecurityContextForInternalAccess(summary.getTenant());
        
        // Reuse the logic in the ModelSummaryResource to do the updates
        ModelSummaryResource msr = new ModelSummaryResource();
        msr.setModelSummaryEntityMgr(modelSummaryEntityMgr);
        return msr.update(modelId, attrMap);
    }
    
    @RequestMapping(value = "/{op}/{left}/{right}", method = RequestMethod.GET)
    @ResponseBody
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
    
}
