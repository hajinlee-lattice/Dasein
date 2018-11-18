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
import com.latticeengines.hadoop.service.EMRCacheService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "redirect", description = "Redirect links")
@RestController
@RequestMapping("/" + REDIRECT_RESOURCE)
public class RedirectResource {

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private EMRCacheService emrCacheService;

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

    @RequestMapping("/emr/rm")
    @ApiOperation("Redirect to EMR RM")
    @NoCustomerSpace
    public ModelAndView redirectRM() {
        String masterIp = emrCacheService.getMasterIp();
        if (StringUtils.isNotBlank(masterIp)) {
            String url = String.format("http://%s:8088", masterIp);
            return new ModelAndView("redirect:" + url);
        } else {
            return new ModelAndView("Cannot find the master ip of emr cluster in current stack.");
        }
    }

    @RequestMapping("/emr/hdfs")
    @ApiOperation("Redirect to EMR HDFS")
    @NoCustomerSpace
    public ModelAndView redirectHdfs() {
        String masterIp = emrCacheService.getMasterIp();
        if (StringUtils.isNotBlank(masterIp)) {
            String url = String.format("http://%s:50070", masterIp);
            return new ModelAndView("redirect:" + url);
        } else {
            return new ModelAndView("Cannot find the master ip of emr cluster in current stack.");
        }
    }

}
