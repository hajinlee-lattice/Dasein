package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.JourneyStageEntityMgr;
import com.latticeengines.apps.cdl.service.JourneyStageService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStagePredicate;
import com.latticeengines.domain.exposed.cdl.activity.StreamFieldToFilter;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("JourneyStageService")
public class JourneyStageServiceImpl implements JourneyStageService {

    private static final Logger log = LoggerFactory.getLogger(JourneyStageServiceImpl.class);

    @Inject
    private JourneyStageEntityMgr journeyStageEntityMgr;

    @Override
    public JourneyStage findByPid(String customerSpace, Long pid) {
        return journeyStageEntityMgr.findByPid(pid);
    }

    @Override
    public JourneyStage findByStageName(String customerSpace, String stageName) {
        Tenant tenant = MultiTenantContext.getTenant();
        return journeyStageEntityMgr.findByTenantAndStageName(tenant, stageName);
    }

    @Override
    public List<JourneyStage> findByTenant(String customerSpace) {
        Tenant tenant = MultiTenantContext.getTenant();
        return journeyStageEntityMgr.findByTenant(tenant);
    }

    @Override
    public JourneyStage createOrUpdate(String customerSpace, JourneyStage journeyStage) {
        JourneyStage newJourneyStage = null;
        if (journeyStage.getPid() != null) {
            newJourneyStage = journeyStageEntityMgr.findByPid(journeyStage.getPid());
        }
        if (newJourneyStage == null) {
            newJourneyStage = new JourneyStage();
        }
        newJourneyStage.setTenant(MultiTenantContext.getTenant());
        newJourneyStage.setPriority(journeyStage.getPriority());
        newJourneyStage.setStageName(journeyStage.getStageName());
        newJourneyStage.setDisplayName(journeyStage.getDisplayName());
        newJourneyStage.setPredicates(journeyStage.getPredicates());
        journeyStageEntityMgr.createOrUpdate(newJourneyStage);
        return newJourneyStage;
    }

    @Override
    public void createDefaultJourneyStages(String customerSpace) {
        createDefaultJourneyStages();
    }

    @Override
    public void delete(String customerSpace, JourneyStage journeyStage) {
        JourneyStage oldJourneyStage = journeyStageEntityMgr.findByPid(journeyStage.getPid());
        if (oldJourneyStage == null) {
            log.error("cannot find JourneyStage in tenant {}, journeyStage name is {}, pid is {}.", customerSpace,
                    journeyStage.getStageName(), journeyStage.getPid());
            return;
        }
        journeyStageEntityMgr.delete(oldJourneyStage);
    }

    private void createDefaultJourneyStages() {
        Tenant tenant = MultiTenantContext.getTenant();
        createDefaultStage(tenant);
        createJourneyStage(tenant, "Aware", 6, AtlasStream.StreamType.DnbIntentData, 1, 28, Collections.emptyList());
        createJourneyStage(tenant, "Engaged", 5, AtlasStream.StreamType.WebVisit, 1, 14, Collections.emptyList());

        StreamFieldToFilter filter = new StreamFieldToFilter();
        filter.setComparisonType(ComparisonType.EQUAL);
        filter.setColumnName(InterfaceName.EventType);
        filter.setColumnValue("WebVisit");
        createJourneyStage(tenant, "Known Engaged", 4, AtlasStream.StreamType.MarketingActivity, 1, 14,
                Collections.singletonList(filter));

        filter = new StreamFieldToFilter();
        filter.setComparisonType(ComparisonType.NOT_CONTAINS);
        filter.setColumnName(InterfaceName.Detail1);
        filter.setColumnValue("Closed%");
        createJourneyStage(tenant, "Opportunity", 3, AtlasStream.StreamType.Opportunity, 1, 90,
                Collections.singletonList(filter));

        filter = new StreamFieldToFilter();
        filter.setComparisonType(ComparisonType.CONTAINS);
        filter.setColumnName(InterfaceName.Detail1);
        filter.setColumnValue("Closed%");
        createJourneyStage(tenant, "Closed", 2, AtlasStream.StreamType.Opportunity, 1, 90,
                Collections.singletonList(filter));

        StreamFieldToFilter filter2 = new StreamFieldToFilter();
        filter2.setComparisonType(ComparisonType.CONTAINS);
        filter2.setColumnName(InterfaceName.Detail1);
        filter2.setColumnValue("%Won");
        createJourneyStage(tenant, "Closed-Won", 1, AtlasStream.StreamType.Opportunity, 1, 90,
                Arrays.asList(filter, filter2));
    }

    private void createJourneyStage(Tenant tenant, String stageName, int priority, AtlasStream.StreamType streamType,
            int noOfEvents, int periodDays, List<StreamFieldToFilter> filters) {
        JourneyStagePredicate predicates = new JourneyStagePredicate();
        predicates.setStreamType(streamType);
        predicates.setPeriodDays(periodDays);
        predicates.setNoOfEvents(noOfEvents);
        predicates.setStreamFieldsToFilter(filters);
        JourneyStage journeyStage = new JourneyStage.Builder().withTenant(tenant).withStageName(stageName)
                .withDisplayName(stageName).withPriority(priority).withPredicates(Collections.singletonList(predicates))
                .build();
        journeyStageEntityMgr.createOrUpdate(journeyStage);
    }

    private void createDefaultStage(Tenant tenant) {
        JourneyStagePredicate predicates = new JourneyStagePredicate();
        predicates.setStreamType(AtlasStream.StreamType.DefaultStage);
        JourneyStage journeyStage = new JourneyStage.Builder().withTenant(tenant).withPriority(7).withStageName("Dark")
                .withDisplayName("Dark").withPredicates(Collections.singletonList(predicates)).build();
        journeyStageEntityMgr.createOrUpdate(journeyStage);
    }
}
