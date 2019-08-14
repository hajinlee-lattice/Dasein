package com.latticeengines.pls.controller.datacollection;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "contacts", description = "REST resource for serving data about contacts")
@RestController
@RequestMapping("/contacts")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class ContactResource extends BaseFrontEndEntityResource {

    private static final Logger log = LoggerFactory.getLogger(ContactResource.class);

    @Inject
    public ContactResource(EntityProxy entityProxy, SegmentProxy segmentProxy, DataCollectionProxy dataCollectionProxy,
            GraphDependencyToUIActionUtil graphDependencyToUIActionUtil) {
        super(entityProxy, segmentProxy, dataCollectionProxy, graphDependencyToUIActionUtil);
    }

    @Deprecated
    @Override
    @RequestMapping(value = "/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public Long getCount(@RequestBody(required = false) FrontEndQuery frontEndQuery) {
        try {
            return super.getCount(frontEndQuery);
        } catch (Exception e) {
            log.error("Failed to get contact count", e);
            throw new LedpException(LedpCode.LEDP_36002);
        }
    }

    @Override
    @RequestMapping(value = "/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@RequestBody(required = false) FrontEndQuery frontEndQuery) {
        try {
            return super.getData(frontEndQuery);
        } catch (Exception e) {
            log.error("Failed to get contact data", e);
            throw new LedpException(LedpCode.LEDP_36002);
        }
    }

    @Override
    BusinessEntity getMainEntity() {
        return BusinessEntity.Contact;
    }

    @Override
    List<Lookup> getDataLookups() {
        return Arrays.asList(//
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactName.name()), //
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.CompanyName.name()), //
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.Email.name()));
    }

}
