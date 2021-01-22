package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.domain.GenericFetchResult;
import com.latticeengines.datacloud.match.entitymgr.ContactTpsEntryMgr;
import com.latticeengines.datacloud.match.entitymgr.impl.ContactTpsEntryMgrImpl;
import com.latticeengines.datacloud.match.repository.reader.ContactMasterTpsColumnRepository;
import com.latticeengines.datacloud.match.service.TpsFetchService;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.datacloud.contactmaster.LiveRampDestination;
import com.latticeengines.domain.exposed.datacloud.match.ContactTpsEntry;
import com.latticeengines.domain.exposed.datacloud.match.config.TpsMatchConfig;

@Lazy
@Service
public class TpsFetchServiceImpl implements TpsFetchService {

    private static final Logger log = LoggerFactory.getLogger(TpsFetchServiceImpl.class);

    @Inject
    private FabricDataService dataService;

    @Inject
    private ContactMasterTpsColumnRepository cmTpsColumnRepository;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    private Map<String, ContactTpsEntryMgr> tpsEntityMgrs = new HashMap<>();

    @Override
    public List<GenericFetchResult> fetchAndFilter(Collection<String> recordIds, TpsMatchConfig matchConfig) {
        return tpsFetchAndFilter(recordIds, matchConfig);
    }

    public List<GenericFetchResult> tpsFetchAndFilter(@NotNull Collection<String> recordIds,
            TpsMatchConfig matchConfig) {
        List<GenericFetchResult> results = new ArrayList<>();
        ContactTpsEntryMgr tpsEntityMgr = getTpsEntityMgr();
        List<String> ids = new ArrayList<>(recordIds);
        String msg = String.format("Batch getting %d items in dynamo", recordIds.size());
        try (PerformanceTimer timer = new PerformanceTimer(msg)) {
            List<ContactTpsEntry> entries = tpsEntityMgr.batchFindByKey(ids);
            for (ContactTpsEntry entry : entries) {
                if ((entry != null) && (entry.getAttributes() != null)) {
                    GenericFetchResult fetchResult = new GenericFetchResult();
                    fetchResult.setRecordId(entry.getId());
                    fetchResult.setResult(entry.getAttributes());

                    if (matchConfig == null || isValid(fetchResult, matchConfig)) {
                        results.add(fetchResult);
                    }
                }
            }
        }

        return results;
    }

    public ContactTpsEntryMgr getTpsEntityMgr() {
        String version = versionEntityMgr.currentApprovedVersion().getVersion();
        log.debug("TpsFetchServiceImpl, datacloud version " + version);
        ContactTpsEntryMgr entityMgr = tpsEntityMgrs.get(version);
        if (entityMgr == null) {
            entityMgr = new ContactTpsEntryMgrImpl(dataService, version);
            entityMgr.init();
            tpsEntityMgrs.put(version, entityMgr);
        }
        return entityMgr;
    }

    // filter 1% and set title and job function
    public List<GenericFetchResult> mockFetch(@NotNull Collection<String> recordIds, TpsMatchConfig matchConfig) {
        Random random = new Random(4);
        List<GenericFetchResult> results = new ArrayList<>();
        for (String recordId : recordIds) {
            if (random.nextInt(100) == 1) {
                GenericFetchResult fetchResult = new GenericFetchResult();
                fetchResult.setRecordId(recordId);
                fetchResult.setResult(ImmutableMap.of(ContactMasterConstants.TPS_ATTR_RECORD_ID, recordId));
                // noinspection ConstantConditions
                if (matchConfig == null || isValid(fetchResult, matchConfig)) {
                    results.add(fetchResult);
                }
            }
        }
        return results;
    }

    private boolean isValid(GenericFetchResult fetchResult, TpsMatchConfig matchConfig) {
        List<String> filterFunctions = matchConfig.getJobFunctions();
        List<String> filterLevels = matchConfig.getJobLevels();
        LiveRampDestination destination = matchConfig.getDestination();
        Map<String, Object> result = fetchResult.getResult();

        // Filter by job function
        String jobFunction = result.get(ContactMasterConstants.TPS_STANDARD_JOB_FUNCTION).toString();
        if ((filterFunctions != null) && !filterFunctions.contains(jobFunction)) {
            return false;
        }
        // Filter by job level
        String jobLevel = result.get(ContactMasterConstants.TPS_STANDARD_JOB_LEVEL).toString();
        if ((filterLevels != null) && !filterLevels.contains(jobLevel)) {
            return false;
        }
        // Filter by match destination
        if (destination != null) {
            String columnName = cmTpsColumnRepository.findColumnNameByMatchDestination(destination.name());
            if (result.get(columnName) == null) {
                return false;
            } else {
                String dest = result.get(columnName).toString().toLowerCase();
                if (!Arrays.asList("t", "true").contains(dest)) {
                    return false;
                }
            }
        }

        return true;
    }
}
