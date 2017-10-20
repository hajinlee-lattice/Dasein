package com.latticeengines.domain.exposed.query;

import java.util.List;
import java.util.Map;

public class UnionLookup extends Lookup {
    private LogicalRestriction rootRestriction;

    private Map<LogicalRestriction, List<Lookup>> lookupMap;

    public LogicalRestriction getRootRestriction() {
        return rootRestriction;
    }

    public void setRootRestriction(LogicalRestriction rootRestriction) {
        this.rootRestriction = rootRestriction;
    }

    public Map<LogicalRestriction, List<Lookup>> getLookupMap() {
        return lookupMap;
    }

    public void setLookupMap(Map<LogicalRestriction, List<Lookup>> lookupMap) {
        this.lookupMap = lookupMap;
    }
}
