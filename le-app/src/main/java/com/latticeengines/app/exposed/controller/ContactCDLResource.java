package com.latticeengines.app.exposed.controller;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.QueryDecorator;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "contacts", description = "REST resource for serving data about contacts")
@RestController
@RequestMapping("/contacts")
public class ContactCDLResource extends BaseFrontEndEntityResource {

    @RequestMapping(value = "/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getCount(@RequestBody FrontEndQuery frontEndQuery) {
        return super.getCount(BusinessEntity.Contact, frontEndQuery);
    }

    @RequestMapping(value = "/count/restriction", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified restriction")
    public long getCountForRestriction(@RequestBody FrontEndRestriction restriction) {
        return super.getCountForRestriction(BusinessEntity.Contact, restriction);
    }

    @Override
    @RequestMapping(value = "/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@RequestBody FrontEndQuery frontEndQuery) {
        return super.getData(frontEndQuery);
    }

    @Override
    public QueryDecorator getQueryDecorator(boolean addSelects) {
        return new QueryDecorator() {

            @Override
            public BusinessEntity getLookupEntity() {
                return BusinessEntity.Contact;
            }

            @Override
            public String[] getEntityLookups() {
                return new String[] { "SalesforceAccountID" };
            }

            @Override
            public String[] getLDCLookups() {
                return new String[] {};
            }

            @Override
            public BusinessEntity getFreeTextSearchEntity() {
                return BusinessEntity.Contact;
            }

            @Override
            public String[] getFreeTextSearchAttrs() {
                return new String[] { "" };
            }

            @Override
            public boolean addSelects() {
                return addSelects;
            }
        };
    }

}
