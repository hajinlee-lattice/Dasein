package com.latticeengines.security.service;

import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.security.Credentials;

public interface IDaaSService {

    LoginDocument login(Credentials credentials);

}
