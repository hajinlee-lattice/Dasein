package com.latticeengines.api.controller;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.api.ThrottleSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.LoadConfiguration;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

@Controller
public class ModelResource {
    private static final Log log = LogFactory.getLog(ModelResource.class);

    @Autowired
    private ModelingService modelingService;
    
    public ModelResource() {
        // Need to set java.class.path in order for the Sqoop dynamic java compilation to work
        log.info("Java Home = " + System.getProperty("java.home"));
        System.setProperty("java.class.path", System.getProperty("java.class.path") + addToClassPath());
        log.info("Class path = " + System.getProperty("java.class.path"));
    }

    private String addToClassPath() {
        return ":" + System.getProperty("jetty.class.path");
    }

    @RequestMapping(value = "/submit", method = RequestMethod.POST, headers = "Accept=application/xml, application/json")
    @ResponseBody
    public AppSubmission submit(@RequestBody Model model) {
        AppSubmission submission = new AppSubmission(modelingService.submitModel(model));
        return submission;
    }

    @RequestMapping(value = "/createSamples", method = RequestMethod.POST, headers = "Accept=application/xml, application/json")
    @ResponseBody
    public AppSubmission createSamples(@RequestBody SamplingConfiguration config) {
        AppSubmission submission = new AppSubmission(Arrays.<ApplicationId> asList(modelingService
                .createSamples(config)));
        return submission;
    }

    @RequestMapping(value = "/throttle", method = RequestMethod.POST, headers = "Accept=application/xml, application/json")
    @ResponseBody
    public ThrottleSubmission throttle(@RequestBody ThrottleConfiguration config) {
        modelingService.throttle(config);
        return new ThrottleSubmission(config.isImmediate());
    }

    @RequestMapping(value = "/load", method = RequestMethod.POST, headers = "Accept=application/xml, application/json")
    @ResponseBody
    public AppSubmission loadData(@RequestBody LoadConfiguration config) {
        AppSubmission submission = new AppSubmission(modelingService.loadData(config));
        return submission;
    }
    
    @RequestMapping(value = "/getjobstatus/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/xml, application/json")
    @ResponseBody
    public JobStatus getJobStatus(@PathVariable String applicationId) {
        return modelingService.getJobStatus(applicationId);
    }
    
    @RequestMapping(value = "/features", method = RequestMethod.POST, headers = "Accept=application/xml, application/json")
    @ResponseBody
    public StringList getFeatures(@RequestBody Model model) {
        return new StringList(modelingService.getFeatures(model, true));
    }
}
