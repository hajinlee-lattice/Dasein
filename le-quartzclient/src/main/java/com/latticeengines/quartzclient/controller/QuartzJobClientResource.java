package com.latticeengines.quartzclient.controller;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.quartz.QuartzJobArguments;
import com.latticeengines.domain.exposed.quartz.TriggeredJobInfo;
import com.latticeengines.quartzclient.service.QuartzJobService;

@RestController
@RequestMapping("/quartzjob")
@ComponentScan({ "com.latticeengines.quartzclient.service" })
public class QuartzJobClientResource {

    @Inject
    private QuartzJobService quartzJobService;

    @PostMapping("/triggerjob")
    @ResponseBody
    public TriggeredJobInfo triggerJob(@RequestBody QuartzJobArguments jobArgs,
            HttpServletRequest request) {
        return quartzJobService.runJob(jobArgs);
    }

    @PostMapping("/checkactivejob")
    @ResponseBody
    public Boolean checkActiveJob(@RequestBody QuartzJobArguments jobArgs,
            HttpServletRequest request) {
        return quartzJobService.hasActiveJob(jobArgs);
    }

    @PostMapping("/checkjobbean")
    @ResponseBody
    public Boolean checkJobBean(@RequestBody QuartzJobArguments jobArgs, HttpServletRequest request) {
        return quartzJobService.jobBeanExist(jobArgs);
    }

}
