package com.latticeengines.domain.exposed.scoringapi;

import java.util.List;

public interface Warnings {

    void addWarning(Warning warning);

    void addWarning(String recordId, Warning warning);

    List<Warning> getWarnings();

    List<Warning> getWarnings(String recordId);

    boolean hasWarnings();

    boolean hasWarnings(String recordId);
}
