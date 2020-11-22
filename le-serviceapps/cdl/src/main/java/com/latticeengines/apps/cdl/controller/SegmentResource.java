package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.annotation.Action;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.apps.cdl.workflow.ImportListSegmentWorkflowSubmitter;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.cdl.CreateDataTemplateRequest;
import com.latticeengines.domain.exposed.cdl.ListSegmentImportRequest;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata segments")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/segments")
public class SegmentResource {

    private static final Logger log = LoggerFactory.getLogger(SegmentResource.class);

    @Inject
    private SegmentService segmentService;

    @Inject
    private ActionService actionService;

    @Inject
    private ImportListSegmentWorkflowSubmitter listSegmentWorkflowSubmitter;

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Get all segments")
    public List<MetadataSegment> getSegments(@PathVariable String customerSpace) {
        return segmentService.getSegments();
    }

    @GetMapping("/list")
    @ResponseBody
    @ApiOperation(value = "Get all list segments")
    public List<MetadataSegment> getListSegments(@PathVariable String customerSpace) {
        return segmentService.getListSegments();
    }

    @GetMapping("/{segmentName}")
    @ResponseBody
    @ApiOperation(value = "Get segment by name")
    public MetadataSegment getSegment(@PathVariable String customerSpace, @PathVariable String segmentName) {
        return segmentService.findByName(segmentName);
    }

    @GetMapping("/{segmentName}/dependencies")
    @ResponseBody
    @ApiOperation(value = "Get all the dependencies")
    public Map<String, List<String>> getDependencies(@PathVariable String customerSpace,
                                                     @PathVariable String segmentName) throws Exception {
        log.info(String.format("get all dependencies for segmentName=%s", segmentName));
        return segmentService.getDependencies(segmentName);
    }

    @GetMapping("/pid/{segmentName}")
    @ResponseBody
    @ApiOperation(value = "Get segment with pid by name")
    public MetadataSegmentDTO getSegmentWithPid(@PathVariable String customerSpace, @PathVariable String segmentName) {
        MetadataSegmentDTO metadataSegmentDTO = new MetadataSegmentDTO();
        MetadataSegment segment = segmentService.findByName(segmentName);
        metadataSegmentDTO.setMetadataSegment(segment);
        metadataSegmentDTO.setPrimaryKey(segment.getPid());
        return metadataSegmentDTO;
    }

    @PostMapping("")
    @ResponseBody
    @Action
    @ApiOperation(value = "Create or update a segment")
    public MetadataSegment createOrUpdateSegment(@PathVariable String customerSpace,
                                                 @RequestBody MetadataSegment segment,
                                                 @RequestParam(value = "user", required = false, defaultValue = "DEFAULT_USER") String user) {
        MetadataSegment res = segmentService.createOrUpdateSegment(segment);
        actionService.registerAction(ActionContext.getAction(), user);
        return res;
    }

    @PostMapping("/list")
    @ResponseBody
    @ApiOperation(value = "Create or update a list segment")
    public MetadataSegment createOrUpdateListSegment(@PathVariable String customerSpace, @RequestBody MetadataSegment segment) {
        MetadataSegment res = segmentService.createOrUpdateListSegment(segment);
        return res;
    }

    @PutMapping("/list/listsegment")
    @ResponseBody
    @ApiOperation(value = "Only update list segment entity under metadata segment")
    public ListSegment updateListSegment(@PathVariable String customerSpace, @RequestBody ListSegment listSegment) {
        return segmentService.updateListSegment(listSegment);
    }

    @GetMapping("/list/{externalSystem}/{externalSegmentId}")
    @ResponseBody
    @ApiOperation(value = "Get list segment by external info")
    public MetadataSegment getListSegmentByExternalInfo(@PathVariable String customerSpace, @PathVariable String externalSystem, @PathVariable String externalSegmentId) {
        return segmentService.findByExternalInfo(externalSystem, externalSegmentId);
    }

    @GetMapping("/list/{segmentName}")
    @ResponseBody
    @ApiOperation(value = "Get list segment by name")
    public MetadataSegment getListSegmentByName(@PathVariable String customerSpace, @PathVariable String segmentName) {
        return segmentService.findListSegmentByName(segmentName);
    }

    @DeleteMapping("/list/{externalSystem}/{externalSegmentId}")
    @ApiOperation(value = "Delete a list segment by external info")
    public boolean deleteSegmentByExternalInfo(@PathVariable String customerSpace,
                                               @PathVariable String externalSystem, @PathVariable String externalSegmentId,
                                               @RequestParam(value = "hard-delete", required = false, defaultValue = "false") Boolean hardDelete) {
        return segmentService.deleteSegmentByExternalInfo(externalSystem, externalSegmentId, hardDelete);
    }

    @PostMapping("/list/{segmentName}/datatemplate")
    @ResponseBody
    @ApiOperation(value = "Create or update a date template for list segment")
    public String createOrUpdateDataUnit(@PathVariable String customerSpace,
                                         @PathVariable String segmentName, @RequestBody CreateDataTemplateRequest segment) {
        return segmentService.createOrUpdateDataTemplate(segmentName, segment);
    }

    @DeleteMapping("/{segmentName}")
    @ApiOperation(value = "Delete a segment by name")
    public Boolean deleteSegmentByName(@PathVariable String customerSpace,
                                       @PathVariable String segmentName,
                                       @RequestParam(value = "hard-delete", required = false, defaultValue = "false") Boolean hardDelete) {
        return segmentService.deleteSegmentByName(segmentName, false, hardDelete);
    }

    @PutMapping("/{segmentName}/revertdelete")
    @ResponseBody
    @ApiOperation(value = "Revert segment deletion given its name")
    public Boolean revertDeleteRatingEngine(@PathVariable String customerSpace,
                                            @PathVariable String segmentName) {
        segmentService.revertDeleteSegmentByName(segmentName);
        return true;
    }

    @GetMapping("/deleted")
    @ResponseBody
    @ApiOperation(value = "Get all deleted segments")
    public List<String> getAllDeletedSegments(@PathVariable String customerSpace) {
        return segmentService.getAllDeletedSegments();
    }

    @GetMapping("/{segmentName}/stats")
    @ResponseBody
    @ApiOperation(value = "Get segment by name")
    public StatisticsContainer getSegmentStats(@PathVariable String customerSpace, @PathVariable String segmentName,
                                               @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return segmentService.getStats(segmentName, version);
    }

    @PostMapping("/{segmentName}/stats")
    @ResponseBody
    @ApiOperation(value = "Upsert stats to a segment")
    public SimpleBooleanResponse upsertStatsToSegment(@PathVariable String customerSpace,
                                                      @PathVariable String segmentName, @RequestBody StatisticsContainer statisticsContainer) {
        segmentService.upsertStats(segmentName, statisticsContainer);
        return SimpleBooleanResponse.successResponse();
    }

    @PutMapping("/{segmentName}/counts")
    @ResponseBody
    @ApiOperation(value = "Update counts for a segment")
    public Map<BusinessEntity, Long> updateSegmentCount(@PathVariable String customerSpace,
                                                        @PathVariable String segmentName) {
        return segmentService.updateSegmentCounts(segmentName);
    }

    @PutMapping("/counts/async")
    @ResponseBody
    @ApiOperation(value = "Update counts for all segment")
    public void updateAllCountsAsync(@PathVariable String customerSpace) {
        segmentService.updateSegmentsCountsAsync();
    }

    @PostMapping("/attributes")
    @ResponseBody
    @ApiOperation(value = "get attributes for segments")
    public List<AttributeLookup> findDependingAttributes(@PathVariable String customerSpace,
                                                         @RequestBody List<MetadataSegment> metadataSegments) {
        return segmentService.findDependingAttributes(metadataSegments);
    }

    @GetMapping("/export/{exportId}")
    @ApiOperation(value = "Get Segment export job info.")
    public MetadataSegmentExport getMetadataSegmentExport(@PathVariable("customerSpace") String customerSpace, //
                                                          @PathVariable("exportId") String exportId) {
        log.debug(String.format("Getting MetadataSegmentExport from %s exportId", exportId));
        return segmentService.getMetadataSegmentExport(exportId);
    }

    @PutMapping("/export/{exportId}")
    @ApiOperation(value = "Update segment export job info.")
    public MetadataSegmentExport updateMetadataSegmentExport(@PathVariable("customerSpace") String customerSpace, //
                                                             @PathVariable("exportId") String exportId, //
                                                             @RequestParam("state") MetadataSegmentExport.Status state) {
        log.debug(String.format("Updating MetadataSegmentExport from %s exportId", exportId));
        return segmentService.updateMetadataSegmentExport(exportId, state);
    }

    @GetMapping("/export")
    @ApiOperation(value = "Get all Segment export jobs.")
    public List<MetadataSegmentExport> getMetadataSegmentExports(@PathVariable("customerSpace") String customerSpace) {
        return segmentService.getMetadataSegmentExports();
    }

    @DeleteMapping("/export/{exportId}")
    @ApiOperation(value = "Delete Segment export job info.")
    public void deleteMetadataSegmentExport(@PathVariable("customerSpace") String customerSpace, //
                                            @PathVariable("exportId") String exportId) {
        segmentService.deleteMetadataSegmentExport(exportId);
    }

    @PostMapping("/importListSegment")
    @ResponseBody
    @ApiOperation(value = "start segment import")
    public String importListSegmentCSV(@PathVariable String customerSpace, @RequestBody ListSegmentImportRequest listSegmentImportRequest) {
        ApplicationId appId = listSegmentWorkflowSubmitter.submit(customerSpace, listSegmentImportRequest,
                new WorkflowPidWrapper(-1L));
        return appId.toString();
    }
}
