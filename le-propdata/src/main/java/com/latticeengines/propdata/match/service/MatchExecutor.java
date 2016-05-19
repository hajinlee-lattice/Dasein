package com.latticeengines.propdata.match.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.match.service.impl.MatchContext;

public interface MatchExecutor {

    MatchContext execute(MatchContext matchContext);

    List<MatchContext> execute(List<MatchContext> matchContexts);

    MatchOutput appendMetadata(MatchOutput matchOutput, ColumnSelection.Predefined selection);

}
