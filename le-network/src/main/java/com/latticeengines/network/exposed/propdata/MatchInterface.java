package com.latticeengines.network.exposed.propdata;

import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchVersion;
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

    /**
     * Retrieve entity match version of all environments for desginated tenant
     *
     * @param customerSpace
     *            target tenant
     * @param clearCache
     *            whether to bypass cache and retrieve the latest values
     * @return non {@code null} map of environment -> version
     */
    Map<EntityMatchEnvironment, EntityMatchVersion> getEntityMatchVersions(@NotNull String customerSpace,
            boolean clearCache);

    /**
     * Retrieve entity match version of target environment for desginated tenant
     *
     * @param customerSpace
     *            target tenant
     * @param env
     *            target environment
     * @param clearCache
     *            whether to bypass cache and retrieve the latest values
     * @return non {@code null} environment
     */
    EntityMatchVersion getEntityMatchVersion(@NotNull String customerSpace, @NotNull EntityMatchEnvironment env,
            boolean clearCache);
}
