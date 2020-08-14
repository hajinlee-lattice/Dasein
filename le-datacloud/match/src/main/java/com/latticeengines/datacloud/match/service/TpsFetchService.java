package com.latticeengines.datacloud.match.service;

import java.util.Collection;
import java.util.List;

import com.latticeengines.datacloud.match.domain.GenericFetchResult;
import com.latticeengines.domain.exposed.datacloud.match.config.TpsMatchConfig;

public interface TpsFetchService {

    List<GenericFetchResult> fetchAndFilter(Collection<String> recordIds, TpsMatchConfig matchConfig);

}
