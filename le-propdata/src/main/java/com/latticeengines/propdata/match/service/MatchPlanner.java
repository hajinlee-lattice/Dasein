package com.latticeengines.propdata.match.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.propdata.match.service.impl.MatchContext;

public interface MatchPlanner {

    MatchContext plan(MatchInput input);

    MatchContext plan(MatchInput input, List<ColumnMetadata> metadatas);
}
