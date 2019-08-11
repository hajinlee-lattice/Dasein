package com.latticeengines.apps.cdl.controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ExportToS3Service;
import com.latticeengines.apps.cdl.service.ExportToS3Service.ExportRequest;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ExportToS3Request;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "serving store", description = "REST resource for serving stores")
@RestController
@RequestMapping("/export-to-s3")
public class ExportToS3Resource {

    private static final Logger log = LoggerFactory.getLogger(ExportToS3Resource.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ExportToS3Service exportToS3Service;

    @Resource(name = "commonTaskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @Value("${camille.zk.pod.id}")
    private String podId;

    private ExecutorService workers = null;
    private ConcurrentSkipListSet<String> inProcess = new ConcurrentSkipListSet<>();
    private boolean schedulerStarted = false;

    @NoCustomerSpace
    @RequestMapping(method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Export tenants' artifacts to S3")
    public List<String> exportToS3(@RequestBody ExportToS3Request request) {
        log.info("Starting Export To S3");

        List<String> inputTenants = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(request.getTenants())) {
            request.getTenants().forEach(t -> inputTenants.add(CustomerSpace.parse(t.trim()).getTenantId()));
        }

        log.info("User input tenants=" + inputTenants);
        if (CollectionUtils.isEmpty(inputTenants)) {
            log.warn("There's no input tenants!");
            return Collections.emptyList();
        }

        List<String> resultCustomers = new ArrayList<>();
        for (String tenant: inputTenants) {
            buildCustomers(tenant, resultCustomers);
        }

        if (CollectionUtils.isEmpty(resultCustomers)) {
            log.warn("There's not customers selected!");
        } else {
            startScheduler();
            getWorkers().submit(() -> {
                List<String> customers = new ArrayList<>(resultCustomers);
                int attempt = 0;
                while (CollectionUtils.isNotEmpty(customers)) {
                    log.info("Attempt = " + (++attempt) + " Customers=" + StringUtils.join(customers));
                    customers = new ArrayList<>(submitRequests(customers, Boolean.TRUE.equals(request.getOnlyAtlas())));
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        return resultCustomers;
    }

    private Collection<String> submitRequests(Collection<String> customers, boolean onlyAtlas) {
        ExecutorService workers = getWorkers();
        Set<String> remaining = new HashSet<>();
        customers.forEach(customer -> {
            if (inProcess.contains(customer)) {
                log.info("Exporting for " + customer + " is already in progress.");
            } else if (inProcess.size() > getInProgressParallism()) {
                log.warn("Too many migration tasks in progress, let " + customer + " wait for next attempt.");
                remaining.add(customer);
            } else {
                log.info("Kick off export process for " + customer);
                workers.submit(() -> {
                    try {
                        inProcess.add(customer);
                        log.info("Exporting to S3 for " + customer + ", " + inProcess.size() + " in progress.");
                        List<ExportRequest> requests = new ArrayList<>();
                        exportToS3Service.buildRequests(CustomerSpace.parse(customer), requests, onlyAtlas);
                        exportToS3Service.executeRequests(requests);
                        exportToS3Service.buildDataUnits(CustomerSpace.parse(customer));
                        log.info("Finished Export To S3 for " + customer);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to Export to S3", e);
                    } finally {
                        inProcess.remove(customer);
                    }
                });
            }
        });
        return remaining;
    }

    private void buildCustomers(String tenant, List<String> resultCustomers) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenant);
            String analyticTenant = customerSpace.toString();
            analyticTenant = "/user/s-analytics/customers/" + analyticTenant;
            String contract = customerSpace.getContractId();
            contract = "/Pods/" + podId + "/Contracts/" + contract;
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, analyticTenant) //
                        || HdfsUtils.fileExists(yarnConfiguration, contract)) {
                    if (!resultCustomers.contains(tenant)) {
                        resultCustomers.add(tenant);
                    } else {
                        log.info("Skip " + tenant + " as it does not have analytics or contract folder in hdfs.");
                    }
                }
            } catch (Exception ex) {
                log.warn("Can not find contact dir for tenant=" + tenant);
            }
        } catch (Exception ex) {
            log.error("Can not get the list of files for tenants=" + tenant, ex);
        }
    }

    private ExecutorService getWorkers() {
        if (workers == null) {
            synchronized (this) {
                if (workers == null) {
                    workers = ThreadPoolUtils.getCachedThreadPool("export-s3");
                }
            }
        }
        return workers;
    }

    private int getInProgressParallism() {
        if ("Production".equals(podId)) {
            return 16;
        } else {
            return 4;
        }
    }

    private void startScheduler() {
        if (!schedulerStarted) {
            scheduler.scheduleWithFixedDelay(this::scanInProgress, TimeUnit.MINUTES.toMillis(1));
            schedulerStarted = true;
        }
    }

    private void scanInProgress() {
        log.info("There are " + CollectionUtils.size(inProcess) //
                + " tenants in process: " + StringUtils.join(inProcess));
    }

}
