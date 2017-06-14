package com.latticeengines.datacloud.etl.orchestration.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;

public interface OrchestrationEntityMgr {
    List<Orchestration> findAll();

    Orchestration findByField(String fieldName, Object value);

    void save(Orchestration orch);

    void delete(Orchestration orch);
}
