package com.latticeengines.quartzclient.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.quartz.QuartzJobArguments;
import com.latticeengines.domain.exposed.quartz.TriggeredJobInfo;
import com.latticeengines.quartzclient.service.QuartzJobService;

@RestController
@RequestMapping("/quartzjob")
@ComponentScan({ "com.latticeengines.quartzclient.service" })
public class QuartzJobClientResource {

    @Autowired
    private QuartzJobService quartzJobService;

    @RequestMapping(value = "/triggerjob", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    public TriggeredJobInfo triggerJob(@RequestBody QuartzJobArguments jobArgs,
            HttpServletRequest request) {
        return quartzJobService.runJob(jobArgs);
    }

    @RequestMapping(value = "/checkactivejob", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    public Boolean checkActiveJob(@RequestBody QuartzJobArguments jobArgs,
            HttpServletRequest request) {
        return quartzJobService.hasActiveJob(jobArgs);
    }

    @RequestMapping(value = "/checkjobbean", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    public Boolean checkJobBean(@RequestBody QuartzJobArguments jobArgs, HttpServletRequest request) {
        return quartzJobService.jobBeanExist(jobArgs);
    }

}
