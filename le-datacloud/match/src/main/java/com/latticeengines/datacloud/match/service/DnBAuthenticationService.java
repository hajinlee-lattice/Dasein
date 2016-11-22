package com.latticeengines.datacloud.match.service;

import com.latticeengines.datacloud.match.dnb.DnBKeyType;

public interface DnBAuthenticationService {
    String requestToken(DnBKeyType type);

    void refreshToken(DnBKeyType type);
}
