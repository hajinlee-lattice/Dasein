package com.latticeengines.apps.lp.controller;

import static com.latticeengines.domain.exposed.workflow.WorkflowConstants.LOG_REDIRECT_LINK;
import static com.latticeengines.domain.exposed.workflow.WorkflowConstants.REDIRECT_RESOURCE;

import java.io.IOException;
import java.io.PrintWriter;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.workflowapi.WorkflowLogLinks;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
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
    public ModelAndView redirectWorkflowLog(@PathVariable long workflowPid, HttpServletResponse response) {
        WorkflowLogLinks logLinks = workflowProxy.getLogLinkByWorkflowPid(workflowPid);
        String amUrl = logLinks.getAppMasterUrl();
        String s3Dir = logLinks.getS3LogDir();
        if (StringUtils.isNotBlank(amUrl) && StringUtils.isNotBlank(s3Dir)) {
            try {
                PrintWriter writer = response.getWriter();
                writer.println(String.format("AppMaster URL: %s", amUrl));
                writer.println(String.format("S3 Log Folder: %s", s3Dir));
                writer.close();
                return null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (StringUtils.isNotBlank(s3Dir)) {
            return new ModelAndView("redirect:" + s3Dir);
        } else if (StringUtils.isNotBlank(amUrl)) {
            return new ModelAndView("redirect:" + amUrl);
        } else {
            try {
                PrintWriter writer = response.getWriter();
                writer.println("Cannot find the log link for workflow pid=" + String.valueOf(workflowPid));
                writer.close();
                return null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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

//    // to be used when migrate to webflux
//    private Mono<String> redirectTo(ServerHttpResponse response, String url) {
//        response.setStatusCode(HttpStatus.SEE_OTHER);
//        response.getHeaders().add(HttpHeaders.LOCATION, "/");
//        return response.setComplete().then(Mono.just("Redirected to " + url));
//    }

}
