package com.latticeengines.spark.exposed.service;

import com.latticeengines.domain.exposed.spark.LivySession;

import java.util.Map;

public interface LivySessionService {

    LivySession startSession(String host, String name, Map<String, String> sparkConf);

    LivySession getSession(LivySession session);

    void stopSession(LivySession session);

}
