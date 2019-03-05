package com.latticeengines.api.controller;

import java.util.Arrays;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.api.ThrottleSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelReviewConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "models", description = "REST resource for machine learning models")
@RestController
public class ModelResource {
    private static final Logger log = LoggerFactory.getLogger(ModelResource.class);

    @Inject
    private ModelingService modelingService;

    public ModelResource() {
        // Need to set java.class.path in order for the Sqoop dynamic java
        // compilation to work
        log.info("Java Home = " + System.getProperty("java.home"));
        System.setProperty("java.class.path", System.getProperty("java.class.path") + addToClassPath());
        log.info("Class path = " + System.getProperty("java.class.path"));
    }

    private String addToClassPath() {
        return ":" + System.getProperty("jetty.class.path");
    }

    @RequestMapping(value = "/submit", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Submit models")
    public AppSubmission submit(@RequestBody Model model) {
        AppSubmission submission = new AppSubmission(modelingService.submitModel(model));
        return submission;
    }

    @RequestMapping(value = "/createSamples", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create named samples to be used by profiling or modeling")
    public AppSubmission createSamples(@RequestBody SamplingConfiguration config) {
        AppSubmission submission = new AppSubmission(Arrays.<ApplicationId> asList(modelingService
                .createSamples(config)));
        return submission;
    }

    @RequestMapping(value = "/throttle", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Throttle the number of jobs allowed to run in the cluster", hidden = true)
    public ThrottleSubmission throttle(@RequestBody ThrottleConfiguration config) {
        log.info("Throttle request received.");
        modelingService.throttle(config);
        return new ThrottleSubmission(config.isImmediate());
    }

    @RequestMapping(value = "/resetThrottle", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Reset the internal throttling value", hidden = true)
    public ThrottleSubmission resetThrottle() {
        modelingService.resetThrottle();
        return new ThrottleSubmission();
    }

    @RequestMapping(value = "/load", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Load data from a database table")
    public AppSubmission loadData(@RequestBody LoadConfiguration config) {
        AppSubmission submission = new AppSubmission(Arrays.<ApplicationId> asList(modelingService.loadData(config)));
        return submission;
    }

    @RequestMapping(value = "/getJobStatus/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status about a submitted job")
    public JobStatus getJobStatus(@PathVariable String applicationId) {
        return modelingService.getJobStatus(applicationId);
    }

    @RequestMapping(value = "/profile", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Profile data")
    public AppSubmission profile(@RequestBody DataProfileConfiguration config) {
        AppSubmission submission = new AppSubmission(Arrays.<ApplicationId> asList(modelingService.profileData(config)));
        return submission;
    }

    @RequestMapping(value = "/review", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Review data")
    public AppSubmission review(@RequestBody ModelReviewConfiguration config) {
        AppSubmission submission = new AppSubmission(Arrays.<ApplicationId> asList(modelingService.reviewData(config)));
        return submission;
    }

    @RequestMapping(value = "/features", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of features generated by the data profile")
    public StringList getFeatures(@RequestBody Model model) {
        return new StringList(modelingService.getFeatures(model, false));
    }

}
