package com.latticeengines.pls.controller.datacollection;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "entities", description = "REST resource for serving data about multiple entities")
@RestController
@RequestMapping("/entities")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class EntityResource extends BaseFrontEndEntityResource {

    private static final Logger log = LoggerFactory.getLogger(EntityResource.class);

    @Inject
    public EntityResource(EntityProxy entityProxy, SegmentProxy segmentProxy) {
        super(entityProxy, segmentProxy);
    }

    @RequestMapping(value = "/counts", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public Map<BusinessEntity, Long> getCounts(@RequestBody(required = false) FrontEndQuery frontEndQuery) {
        Map<BusinessEntity, Long> counts = new HashMap<>();
        for (BusinessEntity entity: BusinessEntity.values()) {
            try {
                Long count = getCount(frontEndQuery, entity);
                if (count != null) {
                    counts.put(entity, count);
                }
            } catch (Exception e) {
                log.warn("Failed to get count of " + entity + ": " + e.getMessage());
            }
        }
        return counts;
    }

    @Override
    BusinessEntity getMainEntity() {
        return null;
    }

}
