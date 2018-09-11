package com.latticeengines.datacloud.match.actors.visitor;

import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;

import java.util.Map;

public interface DunsGuideBookLookupService {
    /**
     * Retrieve the number of pending {@link DunsGuideBook} lookup request
     * @return a map with the number of pending requests under key {@link MatchConstants#REQUEST_NUM}
     */
    Map<String, Integer> getPendingReqStats();
}
