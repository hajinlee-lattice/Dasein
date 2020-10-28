package com.latticeengines.telepath.testartifacts.entities;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.collect.ImmutableList;
import com.latticeengines.telepath.entities.BaseEntity;
import com.latticeengines.telepath.entities.Entity;
import com.latticeengines.telepath.relations.BaseRelation;
import com.latticeengines.telepath.testartifacts.classes.B;
import com.latticeengines.telepath.testartifacts.relations.TestInRelation;
import com.latticeengines.telepath.testartifacts.relations.TestOutRelation;

public class EntityB extends BaseEntity implements Entity {

    public static final String TYPE = "B";

    public static final String PROP_FK_C = "foreignKeyC";

    private final Map<String, Object> props = new HashMap<>();

    public EntityB() {
    }

    public EntityB(B b) {
        extractPropsFromObj(b);
    }

    @Override
    public String getEntityType() {
        return TYPE;
    }

    @Override
    public List<Pair<? extends BaseEntity, ? extends BaseRelation>> getEntityRelations() {
        return ImmutableList.of( //
                Pair.of(new EntityA(), new TestInRelation()), //
                Pair.of(new EntityC(), new TestOutRelation()) //
        );
    }

    @Override
    public void extractPropsFromObj(Object obj) {
        B b = (B) obj;
        props.put(__ENTITY_ID, b.getId());
        props.put(__NAMESPACE, b.getNamespace());
        props.put(PROP_FK_C, b.getForeignKeyC());
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
