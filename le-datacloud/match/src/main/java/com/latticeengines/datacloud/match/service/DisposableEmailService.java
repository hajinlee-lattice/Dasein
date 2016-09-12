package com.latticeengines.datacloud.match.service;

import java.io.Serializable;

public interface DisposableEmailService extends Serializable {

    Boolean isDisposableEmailDomain(String domain);

}
