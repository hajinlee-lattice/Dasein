package com.latticeengines.datacloud.match.actors.visitor;

import java.util.Map;

public interface DnBLookupService {
    Map<String, Integer> getUnsubmittedStats();

    Map<String, Integer> getSubmittedStats();

    Map<String, Integer> getFinishedStats();
}
