package com.latticeengines.scoringapi.exposed.warnings;

import java.util.List;

public interface Warnings {

    void addWarning(Warning warning);

    List<Warning> getWarnings();

    boolean hasWarnings();

}
