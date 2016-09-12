package com.latticeengines.datacloud.match.service;

import java.io.Serializable;

public interface PublicDomainService extends Serializable {

    Boolean isPublicDomain(String domain);

}
