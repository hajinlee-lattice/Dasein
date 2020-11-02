package com.latticeengines.telepath.testartifacts.entities;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.telepath.entities.BaseEntity;
import com.latticeengines.telepath.entities.Entity;
import com.latticeengines.telepath.relations.BaseRelation;
import com.latticeengines.telepath.testartifacts.classes.C;

public class EntityC extends BaseEntity implements Entity {
    // no namespace
    public static final String TYPE = "C";

    private final Map<String, Object> props = new HashMap<>();

    public EntityC() {
    }

    public EntityC(C c) {
        extractPropsFromObj(c);
    }

    @Override
    public String getEntityType() {
        return TYPE;
    }

    @Override
    public List<Pair<? extends BaseEntity, ? extends BaseRelation>> getEntityRelations() {
        return Collections.emptyList();
    }

    @Override
    public void extractPropsFromObj(Object obj) {
        C c = (C) obj;
        props.put(__ENTITY_ID, c.getId());
    }

    @Override
    public Map<String, Object> getProps() {
        return props;
    }
}
