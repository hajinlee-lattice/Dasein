package com.latticeengines.datacloud.match.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;

public interface DnBAuthenticationService {
    /**
     * Request for DnB token
     *
     * @param type:
     *            DnB key type -- realtime/batch
     * @return token
     */
    String requestToken(@NotNull DnBKeyType type);

    /**
     * Request to refresh DnB token with current expired token provided
     *
     * @param type:
     *            DnB key type -- realtime/batch
     * @param expiredToken
     * @return refreshed token
     */
    String refreshToken(@NotNull DnBKeyType type, @NotNull String expiredToken);
}
