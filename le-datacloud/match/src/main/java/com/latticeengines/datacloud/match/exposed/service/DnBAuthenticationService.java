package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;

public interface DnBAuthenticationService {
    /**
     * Request for DnB token -- If expiredToken is not provided, return local
     * cached token; If expiredToken is provided, compare with local cached
     * token: if different, return local cached token, if same, request token
     * remotely (redis/DnB) to refresh local cached token.
     *
     * @param type:
     *            DnB key type -- realtime/batch
     * @param expiredToken
     * @return token
     */
    String requestToken(@NotNull DnBKeyType type, String expiredToken);

    /**
     * Force to refresh DnB token in redis and local cache of all the envs. Only
     * for MANUAL OPERATION purpose when any unexpected issue happens
     *
     * @param type:
     *            DnB key type -- realtime/batch
     * @param newToken:
     *            If provided, will use this token to overwrite token in redis;
     *            if not provided, request a new token from DnB
     * @return: new token
     */
    String refreshToken(@NotNull DnBKeyType type, String newToken);
}
