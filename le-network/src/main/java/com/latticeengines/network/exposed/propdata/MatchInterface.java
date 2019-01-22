package com.latticeengines.network.exposed.propdata;

import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;

public interface MatchInterface {
    MatchOutput matchRealTime(MatchInput input);
    MatchCommand matchBulk(MatchInput input, String hdfsPod);
    MatchCommand bulkMatchStatus(String rootOperationUid);
    EntityPublishStatistics publishEntity(EntityPublishRequest request);
}
