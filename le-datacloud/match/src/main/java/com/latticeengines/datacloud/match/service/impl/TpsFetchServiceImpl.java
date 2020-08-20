package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants.TPS_ATTR_RECORD_ID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.domain.GenericFetchResult;
import com.latticeengines.datacloud.match.service.TpsFetchService;
import com.latticeengines.domain.exposed.datacloud.contactmaster.LiveRampDestination;
import com.latticeengines.domain.exposed.datacloud.match.config.TpsMatchConfig;

@Lazy
@Service
public class TpsFetchServiceImpl implements TpsFetchService {

    @Override
    public List<GenericFetchResult> fetchAndFilter(Collection<String> recordIds, TpsMatchConfig matchConfig) {
        // FIXME [M39-LiveRamp]: to be changed to fetch from Dynamo and filter
        return mockFetch(recordIds, matchConfig);
    }

    // filter 1% and set title and job function
    public List<GenericFetchResult> mockFetch(@NotNull Collection<String> recordIds, TpsMatchConfig matchConfig) {
        Random random = new Random(4);
        List<GenericFetchResult> results = new ArrayList<>();
        for (String recordId: recordIds) {
            if (random.nextInt(100) == 1) {
                GenericFetchResult fetchResult = new GenericFetchResult();
                fetchResult.setRecordId(recordId);
                fetchResult.setResult(ImmutableMap.of(TPS_ATTR_RECORD_ID, recordId));
                //noinspection ConstantConditions
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
        // FIXME [M39-LiveRamp]: filter result based on the tps config
        return true;
    }

}
