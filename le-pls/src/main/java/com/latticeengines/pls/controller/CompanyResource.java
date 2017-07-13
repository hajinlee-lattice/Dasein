package com.latticeengines.pls.controller;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.Company;
import com.latticeengines.pls.entitymanager.CompanyEntityMgr;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "company", description = "REST resource for companies")
@RestController
@RequestMapping("/companies")
// @PreAuthorize("hasRole('View_PLS_Data')")
public class CompanyResource {

    private static final Logger log = LoggerFactory.getLogger(CompanyResource.class);

    @Autowired
    private CompanyEntityMgr companyEntityMgr;

    @RequestMapping(value = "/count", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get number of companies which meet select criterias")
    public Long getCompanyCount(@RequestParam Map<String,String> reqParams) {

        return companyEntityMgr.findCompanyCount(reqParams);
    }

    @RequestMapping(value = "/{companyId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get detail for specific company")
    public Company getCompany(@PathVariable String companyId) {
        log.info("get company" + companyId);
        return companyEntityMgr.findById(Long.valueOf(companyId));
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of companies which meet select criterias")
    public List<Company> getCompanies(@RequestParam Map<String,String> reqParams) {

        log.info("search company");
        return companyEntityMgr.findCompanies(reqParams);
    }
}
