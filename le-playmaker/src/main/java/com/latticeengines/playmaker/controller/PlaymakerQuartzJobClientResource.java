package com.latticeengines.playmaker.controller;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.quartz.QuartzJobArguments;
import com.latticeengines.domain.exposed.quartz.TriggeredJobInfo;
import com.latticeengines.quartzclient.controller.QuartzJobClientResource;

@RestController
@RequestMapping("/playmaker/quartzjob")
public class PlaymakerQuartzJobClientResource {

    // since playmaker servlet does not start with /playmaker and it causes
    // quartz cluster to not be able to find quartz endpoint. To solve this
    // issue we need this wrapper class for QuartzJobClientResource

    @Inject
    private QuartzJobClientResource quartzJobClientResource;

    @PostMapping("/triggerjob")
    @ResponseBody
    public TriggeredJobInfo triggerJob(@RequestBody QuartzJobArguments jobArgs, HttpServletRequest request) {
        return quartzJobClientResource.triggerJob(jobArgs, request);
    }

    @PostMapping("/checkactivejob")
    @ResponseBody
    public Boolean checkActiveJob(@RequestBody QuartzJobArguments jobArgs, HttpServletRequest request) {
        return quartzJobClientResource.checkActiveJob(jobArgs, request);
    }

    @PostMapping("/checkjobbean")
    @ResponseBody
    public Boolean checkJobBean(@RequestBody QuartzJobArguments jobArgs, HttpServletRequest request) {
        return quartzJobClientResource.checkJobBean(jobArgs, request);
    }

}
