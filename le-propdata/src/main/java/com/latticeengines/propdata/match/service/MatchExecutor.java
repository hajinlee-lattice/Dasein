package com.latticeengines.propdata.match.service;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.match.service.impl.MatchContext;

public interface MatchExecutor {

    MatchContext execute(MatchContext matchContext);
    MatchOutput appendMetadata(MatchOutput matchOutput, ColumnSelection.Predefined selection);

}
