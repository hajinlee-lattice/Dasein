package com.latticeengines.apps.lp.controller;

import static com.latticeengines.domain.exposed.workflow.WorkflowConstants.LOG_REDIRECT_LINK;
import static com.latticeengines.domain.exposed.workflow.WorkflowConstants.REDIRECT_RESOURCE;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "redirect", description = "Redirect links")
@RestController
@RequestMapping("/" + REDIRECT_RESOURCE)
public class RedirectResource {

    @Inject
    private WorkflowProxy workflowProxy;

    @RequestMapping(LOG_REDIRECT_LINK + "{workflowPid}")
    @ApiOperation("Redirect to the link of workflow logs")
    @NoCustomerSpace
    public ModelAndView redirectWorkflowLog(@PathVariable long workflowPid) {
        String url = workflowProxy.getLogLinkByWorkflowPid(workflowPid);
        if (StringUtils.isNotBlank(url)) {
            return new ModelAndView("redirect:" + url);
        } else {
            return new ModelAndView("Cannot find the log link for workflow pid=" //
                    + String.valueOf(workflowPid));
        }
    }

}
