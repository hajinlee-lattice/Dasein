package com.latticeengines.datacloud.match.service;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;

public interface DnBAuthenticationService {
    String requestToken(DnBKeyType type);

    void refreshToken(DnBKeyType type);
}
