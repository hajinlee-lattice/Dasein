package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;

public interface DnBAuthenticationService {
    String requestToken(DnBKeyType type);
    String refreshAndGetToken(DnBKeyType type);
}
