package com.latticeengines.apps.lp.controller;

import static com.latticeengines.domain.exposed.workflow.WorkflowConstants.LOG_REDIRECT_LINK;
import static com.latticeengines.domain.exposed.workflow.WorkflowConstants.REDIRECT_RESOURCE;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.workflowapi.WorkflowLogLinks;
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

    /***
     * This is not a redirect api anymore, because we are not sure if we should redirect to AM or S3
     */
    @RequestMapping(LOG_REDIRECT_LINK + "{workflowPid}")
    @ApiOperation("The links of workflow logs")
    @NoCustomerSpace
    public String redirectWorkflowLog(@PathVariable long workflowPid) {
        WorkflowLogLinks logLinks = workflowProxy.getLogLinkByWorkflowPid(workflowPid);
        List<String> messages = Arrays.asList( //
                "AppMaster URL: " + String.valueOf(logLinks.getAppMasterUrl()), //
                "S3 Log Folder: " + String.valueOf(logLinks.getS3LogDir()));
        return StringUtils.join(messages, "\n");
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
