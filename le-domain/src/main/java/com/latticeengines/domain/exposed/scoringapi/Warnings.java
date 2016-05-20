package com.latticeengines.domain.exposed.scoringapi;

import java.util.List;

public interface Warnings {

    void addWarning(Warning warning);

    List<Warning> getWarnings();

    boolean hasWarnings();

}
