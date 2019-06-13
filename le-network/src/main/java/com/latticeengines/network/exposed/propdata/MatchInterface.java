package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;

public interface MatchInterface {
    MatchOutput matchRealTime(MatchInput input);
    MatchCommand matchBulk(MatchInput input, String hdfsPod);
    MatchCommand bulkMatchStatus(String rootOperationUid);
    EntityPublishStatistics publishEntity(EntityPublishRequest request);

    /**
     * Client for MatchResource#bumpVersion API
     * 
     * @param request
     *            request object, should not be {@literal null}
     * @return current version of all requested environments after bumped up
     */
    BumpVersionResponse bumpVersion(@NotNull BumpVersionRequest request);

    /**
     * Client for MatchResource#publishEntity API
     * 
     * @param requests
     *            requests list, should not be empty
     * @return published statistics for each requests
     */
    List<EntityPublishStatistics> publishEntity(List<EntityPublishRequest> requests);
}
