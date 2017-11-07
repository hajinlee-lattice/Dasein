package com.latticeengines.domain.exposed.camille.Components;

import java.util.HashMap;

public class ComponentsMap extends HashMap<String, HashMap<String, String>> {
    private static final long serialVersionUID = 1L;

    public ComponentsMap() {
        super();
    }

    public ComponentsMap(ComponentsMap m) {
        super(m);
    }

}
