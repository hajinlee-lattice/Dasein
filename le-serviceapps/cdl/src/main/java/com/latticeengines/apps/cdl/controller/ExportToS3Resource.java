package com.latticeengines.apps.cdl.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ExportToS3Service;
import com.latticeengines.apps.cdl.service.ExportToS3Service.ExportRequest;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "serving store", description = "REST resource for serving stores")
@RestController
@RequestMapping("/exportToS3")
public class ExportToS3Resource {

    private static final Logger log = LoggerFactory.getLogger(ExportToS3Resource.class);

    @Inject
    private Configuration yarnConfiguration;
    @Inject
    private ExportToS3Service exportToS3Service;

    @NoCustomerSpace
    @RequestMapping(method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Export tenants' artifacts to S3")
    public List<String> exportToS3( //
            @RequestBody String inputTenants) {
        log.info("Starting Export To S3");
        log.info("User input tenants=" + inputTenants);
        if (StringUtils.isBlank(inputTenants)) {
            log.warn("There's no input tenants!");
            return Collections.emptyList();
        }

        List<CustomerSpace> customerSpaces = new ArrayList<>();
        List<String> resultCustomers = new ArrayList<>();
        String[] inputTenantsToken = inputTenants.split("[,;]");
        for (int i = 0; i < inputTenantsToken.length; i++) {
            buildCustomers(inputTenantsToken[i].trim(), resultCustomers, customerSpaces);
        }

        if (CollectionUtils.isEmpty(resultCustomers)) {
            log.warn("There's not customers selected!");
            return resultCustomers;
        }
        List<ExportRequest> requests = new ArrayList<>();
        for (int i = 0, batchSize = 3; i < resultCustomers.size(); i++) {
            exportToS3Service.buildRequests(customerSpaces.get(i), requests);
            if ((i % batchSize) == (batchSize - 1) || (i == resultCustomers.size() - 1)) {
                exportToS3Service.executeRequests(requests);
                exportToS3Service.buildDataUnits(requests);
                requests.clear();
            }
        }
        log.info("Finished Export To S3");
        return resultCustomers;
    }

    private void buildCustomers(String tenants, List<String> resultCustomers, List<CustomerSpace> customerSpaces) {
        try {
            tenants = "/user/s-analytics/customers/" + tenants;
            List<String> tenantDirs = HdfsUtils.getFilesByGlob(yarnConfiguration, tenants);
            if (CollectionUtils.isEmpty(tenantDirs)) {
                log.warn("There's no tenants folder was selected");
                return;
            }
            tenantDirs.forEach(dir -> {
                String customer = StringUtils.substringAfterLast(dir, "/");
                String tenantBase = StringUtils.substringBeforeLast(dir, "/");
                CustomerSpace customerSpace = CustomerSpace.parse(customer);
                try {
                    if (HdfsUtils.fileExists(yarnConfiguration, tenantBase + "/" + customerSpace.toString())) {
                        if (!resultCustomers.contains(customerSpaces)) {
                            resultCustomers.add(customer);
                            customerSpaces.add(customerSpace);
                        }
                    }
                } catch (Exception ex) {
                    log.warn("Can not find tenant dir for tenant=" + dir);
                }
            });
        } catch (Exception ex) {
            log.error("Can not get the list of files for tenants=" + tenants, ex);
        }
    }

}
