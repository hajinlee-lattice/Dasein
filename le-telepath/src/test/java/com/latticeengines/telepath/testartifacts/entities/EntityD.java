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
import com.latticeengines.telepath.testartifacts.classes.D;
import com.latticeengines.telepath.testartifacts.relations.TestOutRelation;

public class EntityD extends BaseEntity implements Entity {

    public static final String TYPE = "D";

    public static final String PROP_FK_B = "foreignKeyB";

    private final Map<String, Object> props = new HashMap<>();

    public EntityD() {
    }

    public EntityD(D d) {
        extractPropsFromObj(d);
    }

    @Override
    public String getEntityType() {
        return TYPE;
    }

    @Override
    public List<Pair<? extends BaseEntity, ? extends BaseRelation>> getEntityRelations() {
        return ImmutableList.of( //
                Pair.of(new EntityB(), new TestOutRelation()) //
        );
    }

    @Override
    public void extractPropsFromObj(Object obj) {
        D d = (D) obj;
        props.put(__ENTITY_ID, d.getId());
        props.put(__NAMESPACE, d.getNamespace());
        props.put(PROP_FK_B, d.getForeignKeyB());
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
