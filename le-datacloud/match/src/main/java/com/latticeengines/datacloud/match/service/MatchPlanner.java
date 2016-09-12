package com.latticeengines.datacloud.match.service;

import java.util.List;

import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;

public interface MatchPlanner {

    MatchContext plan(MatchInput input);

    MatchContext plan(MatchInput input, List<ColumnMetadata> metadatas, boolean skipExecutionPlanning);
}
