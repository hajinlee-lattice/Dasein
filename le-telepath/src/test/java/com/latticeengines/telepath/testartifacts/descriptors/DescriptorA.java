package com.latticeengines.telepath.testartifacts.descriptors;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.telepath.descriptors.BaseDescriptor;
import com.latticeengines.telepath.descriptors.Descriptor;
import com.latticeengines.telepath.entities.Entity;
import com.latticeengines.telepath.relations.Relation;
import com.latticeengines.telepath.testartifacts.classes.A;
import com.latticeengines.telepath.testartifacts.entities.EntityA;

@Component
public class DescriptorA extends BaseDescriptor<EntityA> implements Descriptor<EntityA> {
    @Override
    protected Class<EntityA> getDependencyEntityClass() {
        return EntityA.class;
    }

    @Override
    public Entity getDependencyEntity(Map<String, Object> props) {
        String id = props.get(EntityA.__ENTITY_ID).toString();
        String namespace = props.get(EntityA.__NAMESPACE).toString();
        A a = new A(id, namespace);
        return new EntityA(a);
    }

    @Override
    public List<Relation> describeLocalEnvironment(Entity entity) {
        return Collections.emptyList();
    }
}
