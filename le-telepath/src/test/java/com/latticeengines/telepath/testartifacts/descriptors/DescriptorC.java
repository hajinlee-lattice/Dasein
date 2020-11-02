package com.latticeengines.telepath.testartifacts.descriptors;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.telepath.descriptors.BaseDescriptor;
import com.latticeengines.telepath.descriptors.Descriptor;
import com.latticeengines.telepath.entities.Entity;
import com.latticeengines.telepath.relations.Relation;
import com.latticeengines.telepath.testartifacts.classes.C;
import com.latticeengines.telepath.testartifacts.entities.EntityA;
import com.latticeengines.telepath.testartifacts.entities.EntityC;

@Component
public class DescriptorC extends BaseDescriptor<EntityC> implements Descriptor<EntityC> {
    // no namespace

    @Override
    protected Class<EntityC> getDependencyEntityClass() {
        return EntityC.class;
    }

    @Override
    public Entity getDependencyEntity(Map<String, Object> props) {
        String id = props.get(EntityA.__ENTITY_ID).toString();
        C c = new C(id);
        return new EntityC(c);
    }

    @Override
    public List<Relation> describeLocalEnvironment(Entity entity) {
        return Collections.emptyList();
    }
}
