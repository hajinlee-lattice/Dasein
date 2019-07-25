package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.latticeengines.domain.exposed.cdl.scheduling.event.Event;
import com.latticeengines.domain.exposed.cdl.scheduling.event.VerifyEvent;
import com.latticeengines.domain.exposed.security.TenantType;

public class SchedulingPASummaryUtil {

    private static final Logger log = LoggerFactory.getLogger(SchedulingPASummaryUtil.class);

    public static String printTenantSummary(SimulationContext simulationContext) {
        List<Long> customerPATimeList = new ArrayList<>();
        List<Long> nonCustomerPATimeList = new ArrayList<>();
        Long custmerMaxPATime = -1L;
        Long customerMinPATime = -1L;
        Long nonCustomerMaxPATime = -1L;
        Long nonCustomerMinPATime = -1L;
        int verifyFailedCount = 0;
        StringBuilder str = new StringBuilder(" ");
        str.append("this simulation summary: schedulingEventCount: ").append(simulationContext.getSchedulingEventCount()).append(", " +
                "dataCloudRefreshCount: ").append(simulationContext.getDataCloudRefreshCount()).append(";").append("\n");
        for (SimulationTenantSummary simulationTenantSummary : simulationContext.getSimulationTenantSummaryMap().values()) {
            List<Event> events = simulationContext.tenantEventMap.get(simulationTenantSummary.getTenantId());
            if (!CollectionUtils.isEmpty(events)) {
                str.append(simulationTenantSummary.getTenantSummary(events));
                str.append("\n");
                str.append(verifyTimeOutImportActionWaitingTime(simulationTenantSummary)).append("\n");
                str.append(verifyTimeOutScheduleNowWaitingTime(simulationTenantSummary)).append("\n");
                if (simulationTenantSummary.getTenantType() == TenantType.CUSTOMER) {
                    customerPATimeList.addAll(simulationTenantSummary.getPaTime());
                    if (simulationTenantSummary.getMaxPATime() > custmerMaxPATime){
                        custmerMaxPATime = simulationTenantSummary.getMaxPATime();
                    }
                    if (customerMinPATime == -1L) {
                        customerMinPATime = simulationTenantSummary.getMinPATime();
                    } else if (customerMinPATime > simulationTenantSummary.getMinPATime()) {
                        customerMinPATime = simulationTenantSummary .getMinPATime();
                    }
                } else {
                    nonCustomerPATimeList.addAll(simulationTenantSummary.getPaTime());
                    if (simulationTenantSummary.getMaxPATime() > nonCustomerMaxPATime){
                        nonCustomerMaxPATime = simulationTenantSummary.getMaxPATime();
                    }
                    if (nonCustomerMinPATime == -1L) {
                        nonCustomerMinPATime = simulationTenantSummary.getMinPATime();
                    } else if (nonCustomerMinPATime > simulationTenantSummary.getMinPATime()) {
                        nonCustomerMinPATime = simulationTenantSummary .getMinPATime();
                    }
                }
            }
        }
        str.append("\n");
        str.append("customerTenant Average PATime is: ").append(getAvgTime(customerPATimeList)).append(", MaxPATime: ").append(custmerMaxPATime).append(", MinPATime: ").append(customerMinPATime);
        str.append("\n");
        str.append("nonCustomerTenant Average PATime is: ").append(getAvgTime(nonCustomerPATimeList)).append(", " +
                "MaxPATime: ").append(nonCustomerMaxPATime).append(", MinPATime: ").append(nonCustomerMinPATime);
        str.append("\n");
        for (VerifyEvent event : simulationContext.verifyEventList) {
            log.debug(event.toString());
            if (!event.isVerifyed()) {
                verifyFailedCount++;
            }
        }
        str.append("verifyFailedCount: " + verifyFailedCount);
        return str.toString();
    }

    private static Long getAvgTime(List<Long> timeList) {
        long avgTime = 0L;
        if (timeList.size() > 0) {
            for (long time : timeList) {
                avgTime += time;
            }
            avgTime = avgTime / timeList.size();
        }
        return avgTime;
    }

    private static String verifyTimeOutImportActionWaitingTime(SimulationTenantSummary simulationTenantSummary) {
        int verifyFailed = 0;
        if (!CollectionUtils.isEmpty(simulationTenantSummary.getFirstImportActionWaitingTime())) {
            for (Long waitingTime : simulationTenantSummary.getFirstImportActionWaitingTime()) {
                if (TenantType.CUSTOMER == simulationTenantSummary.getTenantType()) {
                    if (waitingTime > 4 * 3600 * 1000L) {
                        verifyFailed ++;
                    }
                } else {
                    if (waitingTime > 9 * 3600 * 1000L) {
                        verifyFailed ++;
                    }
                }
            }
        }
        return String.format("tenant %s verifyTimeOutImportActionWaitingTime Failed Count: %d",
                simulationTenantSummary.getTenantId(), verifyFailed);
    }

    private static String verifyTimeOutScheduleNowWaitingTime(SimulationTenantSummary simulationTenantSummary) {
        int verifyFailed = 0;
        if (!CollectionUtils.isEmpty(simulationTenantSummary.getFirstScheduleNowWaitingTime())) {
            for (Long waitingTime : simulationTenantSummary.getFirstScheduleNowWaitingTime()) {
                if (simulationTenantSummary.isLarge() && waitingTime > 9 * 3600 * 1000L) {
                    verifyFailed ++;
                } else if (!simulationTenantSummary.isLarge() && waitingTime > 2 * 3600 * 1000L) {
                    verifyFailed ++;
                }
            }
        }
        return String.format("tenant %s verifyTimeOutScheduleNowWaitingTime Failed Count: %d",
                simulationTenantSummary.getTenantId(), verifyFailed);
    }
}
