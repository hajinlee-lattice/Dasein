package com.latticeengines.datacloud.match.service;

import java.util.List;

import com.latticeengines.datacloud.match.exposed.service.HasDataCloudVersion;
import com.latticeengines.datacloud.match.service.impl.MatchContext;

public interface DbHelper extends HasDataCloudVersion {

    MatchContext sketchExecutionPlan(MatchContext matchContext, boolean skipExecutionPlanning);

    MatchContext fetch(MatchContext context);

    MatchContext fetchSync(MatchContext context);

    List<MatchContext> fetch(List<MatchContext> contexts);

    MatchContext updateInternalResults(MatchContext context);

    MatchContext mergeContexts(List<MatchContext> matchContextList, String dataCloudVersion);

    void splitContext(MatchContext mergedContext, List<MatchContext> matchContextList);

    void initExecutors();

}
