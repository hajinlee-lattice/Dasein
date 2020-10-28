package com.latticeengines.telepath.testartifacts.descriptors;

import static com.latticeengines.telepath.testartifacts.assemblers.TestAssembler.NAMESPACE_0;
import static com.latticeengines.telepath.testartifacts.assemblers.TestAssembler.NAMESPACE_1;
import static com.latticeengines.telepath.testartifacts.assemblers.TestAssembler.OBJ_ID_0;
import static com.latticeengines.telepath.testartifacts.assemblers.TestAssembler.OBJ_ID_1;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.telepath.descriptors.BaseDescriptor;
import com.latticeengines.telepath.descriptors.Descriptor;
import com.latticeengines.telepath.entities.Entity;
import com.latticeengines.telepath.relations.Relation;
import com.latticeengines.telepath.testartifacts.classes.B;
import com.latticeengines.telepath.testartifacts.entities.EntityA;
import com.latticeengines.telepath.testartifacts.entities.EntityB;
import com.latticeengines.telepath.testartifacts.entities.EntityC;
import com.latticeengines.telepath.testartifacts.relations.TestInRelation;
import com.latticeengines.telepath.testartifacts.relations.TestOutRelation;

@Component
public class DescriptorB extends BaseDescriptor<EntityB> implements Descriptor<EntityB> {

    // namespace -> collection of objects
    private static final Map<String, List<B>> preparedObjects = ImmutableMap.of( //
            NAMESPACE_0, Collections.singletonList(new B(OBJ_ID_0, NAMESPACE_0, OBJ_ID_0)), //
            NAMESPACE_1, Collections.singletonList(new B(OBJ_ID_1, NAMESPACE_1, OBJ_ID_1)) //
    );

    @Override
    protected Class<EntityB> getDependencyEntityClass() {
        return EntityB.class;
    }

    @Override
    public List<Entity> getAllEntities(String namespace) {
        return preparedObjects.get(namespace).stream().map(EntityB::new).collect(Collectors.toList());
    }

    @Override
    public Entity getDependencyEntity(Map<String, Object> props) {
        String id = props.get(EntityA.__ENTITY_ID).toString();
        String namespace = props.get(EntityA.__NAMESPACE).toString();
        B b = preparedObjects.get(namespace).stream().filter(obj -> id.equals(obj.getId())).collect(Collectors.toList())
                .get(0);
        return new EntityB(b);
    }

    @Override
    public List<Relation> describeLocalEnvironment(Entity entity) {

        TestInRelation inRelation = new TestInRelation();
        inRelation.setFromEntity(new EntityA());
        inRelation.setFromEntityId(entity.getEntityId());
        inRelation.setToEntity(entity);
        inRelation.setNameSpace(entity.getProps().get(EntityB.__NAMESPACE).toString());

        TestOutRelation outRelation = new TestOutRelation();
        outRelation.setFromEntity(entity);
        outRelation.setToEntity(new EntityC());
        outRelation.setToEntityId(entity.getProps().get(EntityB.PROP_FK_C).toString());
        outRelation.setNameSpace(entity.getProps().get(EntityB.__NAMESPACE).toString());

        return Arrays.asList(inRelation, outRelation);
    }
}
