package com.latticeengines.pls.service;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.domain.exposed.pls.Action;

public interface ActionService {

    Action create(Action action);

    Action update(Action action);

    List<Action> findAll();

    List<Action> findByOwnerId(Long ownerId, Pageable pageable);

    void updateOwnerIdIn(Long ownerId, List<Long> actionPids);

    void delete(Action action);

    Action findByPid(Long pid);

    List<Action> findByPidIn(List<Long> actionPids);

}
