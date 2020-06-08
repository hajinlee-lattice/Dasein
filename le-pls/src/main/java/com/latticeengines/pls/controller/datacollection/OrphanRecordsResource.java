package com.latticeengines.pls.controller.datacollection;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.pls.service.OrphanRecordsService;

import io.swagger.annotations.Api;

@Api(value = "data-collection-orphans", description = "REST resource for orphan records")
@RestController
@RequestMapping("/datacollection/orphans")
public class OrphanRecordsResource {

    @Inject
    private OrphanRecordsService orphanRecordsService;

    @PostMapping("type/{orphanType}/submit")
    @ResponseBody
    public Job submitOrphanRecordsWorkflow(@PathVariable String orphanType, HttpServletResponse response) {
        return orphanRecordsService.submitOrphanRecordsWorkflow(orphanType, response);
    }

    @GetMapping("/orphanexport/{exportId}")
    public void downloadOrphanArtifact(@PathVariable String exportId, HttpServletRequest request, HttpServletResponse response) {
        orphanRecordsService.downloadOrphanArtifact(exportId, request, response);
    }

    @GetMapping("/count")
    public String getOrphanRecordsCount(@RequestParam(required = false) String orphanType) {
        return orphanRecordsService.getOrphanRecordsCount(orphanType);
    }
}
