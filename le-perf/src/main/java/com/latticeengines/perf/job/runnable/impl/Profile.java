package com.latticeengines.perf.job.runnable.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public class Profile extends ModelingResourceJob<DataProfileConfiguration, List<String>> {

    public List<String> executeJob() throws Exception {
        return profile();
    }

    public List<String> profile() throws Exception {
        List<String> applicationIds = rc.profile(config);
        log.info(StringUtils.join(", ", applicationIds));
        return applicationIds;
    }

    public static List<String> createExcludeList() {
        List<String> excludeList = new ArrayList<>();
        excludeList.add("Nutanix_EventTable_Clean");
        excludeList.add("P1_Event");
        excludeList.add("P1_Target");
        excludeList.add("P1_TargetTraining");
        excludeList.add("PeriodID");
        excludeList.add("CustomerID");
        excludeList.add("AwardYear");
        excludeList.add("FundingFiscalQuarter");
        excludeList.add("FundingFiscalYear");
        excludeList.add("BusinessAssets");
        excludeList.add("BusinessEntityType");
        excludeList.add("BusinessIndustrySector");
        excludeList.add("RetirementAssetsYOY");
        excludeList.add("RetirementAssetsEOY");
        excludeList.add("TotalParticipantsSOY");
        excludeList.add("BusinessType");
        excludeList.add("LeadID");
        excludeList.add("Company");
        excludeList.add("Domain");
        excludeList.add("Email");
        excludeList.add("LeadSource");
        excludeList.add("Target");
        return excludeList;
    }
}
