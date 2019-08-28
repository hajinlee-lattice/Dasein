package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.annotation.Action;
import com.latticeengines.apps.cdl.service.ProxyResourceService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata segments")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/segments")
public class SegmentResource {

    private static Logger log = LoggerFactory.getLogger(SegmentResource.class);

    @Inject
    private SegmentService segmentService;

    @Inject
    private ProxyResourceService proxyResourceService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all segments")
    public List<MetadataSegment> getSegments(@PathVariable String customerSpace) {
        return segmentService.getSegments();
    }

    @RequestMapping(value = "/{segmentName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get segment by name")
    public MetadataSegment getSegment(@PathVariable String customerSpace, @PathVariable String segmentName) {
        return segmentService.findByName(segmentName);
    }

    @GetMapping(value = "/{segmentName}/dependencies")
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
        proxyResourceService.registerAction(ActionContext.getAction(), user);
        return res;
    }

    @RequestMapping(value = "/{segmentName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ApiOperation(value = "Delete a segment by name")
    public Boolean deleteSegmentByName(@PathVariable String customerSpace,
            @PathVariable String segmentName,
            @RequestParam(value = "hard-delete", required = false, defaultValue = "false") Boolean hardDelete) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return segmentService.deleteSegmentByName(segmentName, false, hardDelete);
    }

    @PutMapping(value = "/{segmentName}/revertdelete")
    @ResponseBody
    @ApiOperation(value = "Revert segment deletion given its name")
    public Boolean revertDeleteRatingEngine(@PathVariable String customerSpace,
            @PathVariable String segmentName) {
        segmentService.revertDeleteSegmentByName(segmentName);
        return true;
    }

    @GetMapping(value = "/deleted")
    @ResponseBody
    @ApiOperation(value = "Get all deleted segments")
    public List<String> getAllDeletedSegments(@PathVariable String customerSpace) {
        return segmentService.getAllDeletedSegments();
    }

    @RequestMapping(value = "/{segmentName}/stats", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get segment by name")
    public StatisticsContainer getSegmentStats(@PathVariable String customerSpace, @PathVariable String segmentName,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return segmentService.getStats(segmentName, version);
    }

    @RequestMapping(value = "/{segmentName}/stats", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Upsert stats to a segment")
    public SimpleBooleanResponse upsertStatsToSegment(@PathVariable String customerSpace,
            @PathVariable String segmentName, @RequestBody StatisticsContainer statisticsContainer) {
        segmentService.upsertStats(segmentName, statisticsContainer);
        return SimpleBooleanResponse.successResponse();
    }

    @PutMapping(value = "/{segmentName}/counts")
    @ResponseBody
    @ApiOperation(value = "Update counts for a segment")
    public Map<BusinessEntity, Long> updateSegmentCount(@PathVariable String customerSpace,
            @PathVariable String segmentName) {
        return segmentService.updateSegmentCounts(segmentName);
    }

    @PutMapping(value = "/counts")
    @ResponseBody
    @ApiOperation(value = "Update counts for all segment")
    public void updateAllCounts(@PathVariable String customerSpace) {
        Tenant tenant = MultiTenantContext.getTenant();
        new Thread(() -> {
            MultiTenantContext.setTenant(tenant);
            log.info("Start updating counts for all segment for " + MultiTenantContext.getShortTenantId());
            segmentService.updateSegmentsCounts();
        }).start();
    }

    @PostMapping(value = "/attributes")
    @ResponseBody
    @ApiOperation(value = "get attributes for segments")
    public List<AttributeLookup> findDependingAttributes(@PathVariable String customerSpace,
            @RequestBody List<MetadataSegment> metadataSegments) {
        return segmentService.findDependingAttributes(metadataSegments);
    }
}
