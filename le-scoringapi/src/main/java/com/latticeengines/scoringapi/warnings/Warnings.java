package com.latticeengines.scoringapi.warnings;

import java.util.List;

public interface Warnings {

    void addWarning(Warning warning);

    List<Warning> getWarnings();

    boolean hasWarnings();

}
