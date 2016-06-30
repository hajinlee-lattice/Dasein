package com.latticeengines.dataplatform.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.service.ModelReviewService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.ExportConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelReviewConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.network.exposed.dataplatform.ModelInterface;

@Api(value = "models", description = "REST resource for machine learning models")
@RestController
public class ModelResource implements ModelInterface {
    private static final Log log = LogFactory.getLog(ModelResource.class);

    @Autowired
    private ModelingService modelingService;

    @Autowired
    private ModelReviewService modelReviewService;

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

    @RequestMapping(value = "/models", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Submit models")
    public AppSubmission submit(@RequestBody Model model) {
        AppSubmission submission = new AppSubmission(modelingService.submitModel(model));
        return submission;
    }

    @RequestMapping(value = "/samples", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create named samples to be used by profiling or modeling")
    public AppSubmission createSamples(@RequestBody SamplingConfiguration config) {
        AppSubmission submission = new AppSubmission(Arrays.<ApplicationId> asList(modelingService
                .createSamples(config)));
        return submission;
    }

    @RequestMapping(value = "/dataloads", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Load data from a database table")
    public AppSubmission loadData(@RequestBody LoadConfiguration config) {
        AppSubmission submission = new AppSubmission(Arrays.<ApplicationId> asList(modelingService.loadData(config)));
        return submission;
    }

    @RequestMapping(value = "/dataexports", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Export data from HDFS to a database table")
    public AppSubmission exportData(@RequestBody ExportConfiguration config) {
        AppSubmission submission = new AppSubmission(Arrays.<ApplicationId> asList(modelingService.exportData(config)));
        return submission;
    }

    @RequestMapping(value = "/modelingjobs/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status about a submitted job")
    public JobStatus getJobStatus(@PathVariable String applicationId) {
        return modelingService.getJobStatus(applicationId);
    }

    @RequestMapping(value = "/profiles", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Profile data")
    public AppSubmission profile(@RequestBody DataProfileConfiguration config) {
        AppSubmission submission = new AppSubmission(Arrays.<ApplicationId> asList(modelingService.profileData(config)));
        return submission;
    }

    @RequestMapping(value = "/reviews", method = RequestMethod.POST, headers = "Accept=application/json")
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

    @RequestMapping(value = "/reviewdata/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get model review data for a model")
    public ModelReviewData getReviewData(@PathVariable String modelId) {
        return modelReviewService.getReviewData(modelId);
    }

}
