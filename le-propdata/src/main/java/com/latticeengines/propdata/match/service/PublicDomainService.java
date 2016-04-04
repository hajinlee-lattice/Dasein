package com.latticeengines.propdata.match.service;

import java.io.Serializable;

public interface PublicDomainService extends Serializable {

    Boolean isPublicDomain(String domain);

}
