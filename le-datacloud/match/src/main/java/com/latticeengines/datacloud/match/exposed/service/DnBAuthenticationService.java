package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.datacloud.match.dnb.DnBKeyType;

public interface DnBAuthenticationService {
    String requestToken(DnBKeyType type);
    String refreshAndGetToken(DnBKeyType type);
}
