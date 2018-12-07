package com.latticeengines.spark.exposed.service;

import com.latticeengines.domain.exposed.spark.LivySession;

public interface LivySessionService {

    LivySession startSession(String host, String name);

    LivySession getSession(LivySession session);

    void stopSession(LivySession session);

}
