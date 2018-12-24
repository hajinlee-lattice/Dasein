package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.Action;

public interface ActionService {

    Action create(Action action);

    Action cancel(Long actionPid);
}
