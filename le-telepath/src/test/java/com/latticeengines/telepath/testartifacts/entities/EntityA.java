package com.latticeengines.telepath.testartifacts.entities;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.latticeengines.telepath.entities.BaseEntity;
import com.latticeengines.telepath.entities.Entity;
import com.latticeengines.telepath.relations.BaseRelation;
import com.latticeengines.telepath.testartifacts.classes.A;

public class EntityA extends BaseEntity implements Entity {

    public static final String TYPE = "A";

    private final Map<String, Object> props = new HashMap<>();

    public EntityA() {
    }

    public EntityA(A a) {
        extractPropsFromObj(a);
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
        A a = (A) obj;
        props.put(__ENTITY_ID, a.getId());
        props.put(__NAMESPACE, a.getNamespace());
    }

    @Override
    protected void extractSpecificPropsFromVertex(Vertex v) {
        props.put(__NAMESPACE, v.property(__NAMESPACE).value().toString());
    }

    @Override
    public Map<String, Object> getProps() {
        return props;
    }
}
