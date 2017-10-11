package com.latticeengines.pls.controller.datacollection;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "accounts", description = "REST resource for serving data about accounts")
@RestController
@RequestMapping("/accounts")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class AccountResource extends BaseFrontEndEntityResource {

    @Inject
    public AccountResource(EntityProxy entityProxy, SegmentProxy segmentProxy) {
        super(entityProxy, segmentProxy);
    }

    @Override
    @RequestMapping(value = "/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getCount(@RequestBody FrontEndQuery frontEndQuery) {
        try {
            return super.getCount(frontEndQuery);
        } catch (LedpException e) {
            throw e;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_36002, e);
        }
    }

    @Override
    @RequestMapping(value = "/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@RequestBody FrontEndQuery frontEndQuery) {
        try {
            return super.getData(frontEndQuery);
        } catch (LedpException e) {
            throw e;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_36002, e);
        }
    }

    @Override
    @RequestMapping(value = "/ratingcount", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public Map<String, Long> getRatingCount(@RequestBody FrontEndQuery frontEndQuery) {
        try {
            return super.getRatingCount(frontEndQuery);
        } catch (LedpException e) {
            throw e;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_36002, e);
        }
    }

    @Override
    BusinessEntity getMainEntity() {
        return BusinessEntity.Account;
    }

}
