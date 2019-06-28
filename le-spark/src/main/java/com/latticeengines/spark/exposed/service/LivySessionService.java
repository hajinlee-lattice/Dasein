package com.latticeengines.spark.exposed.service;

import java.util.Map;

import com.latticeengines.domain.exposed.spark.LivySession;

public interface LivySessionService {

    LivySession startSession(String name, Map<String, Object> livyConf, Map<String, String> sparkConf);

    LivySession getSession(LivySession session);

    void stopSession(LivySession session);

}
