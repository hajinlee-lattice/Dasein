package com.latticeengines.telepath.testartifacts.descriptors;

import static com.latticeengines.telepath.testartifacts.assemblers.TestAssembler.NAMESPACE_0;
import static com.latticeengines.telepath.testartifacts.assemblers.TestAssembler.NAMESPACE_1;
import static com.latticeengines.telepath.testartifacts.assemblers.TestAssembler.OBJ_ID_0;
import static com.latticeengines.telepath.testartifacts.assemblers.TestAssembler.OBJ_ID_1;

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
import com.latticeengines.telepath.testartifacts.classes.D;
import com.latticeengines.telepath.testartifacts.entities.EntityB;
import com.latticeengines.telepath.testartifacts.entities.EntityD;
import com.latticeengines.telepath.testartifacts.relations.TestOutRelation;

@Component
public class DescriptorD extends BaseDescriptor<EntityD> implements Descriptor<EntityD> {

    // namespace -> collection of objects
    private static final Map<String, List<D>> preparedObjects = ImmutableMap.of( //
            NAMESPACE_0, Collections.singletonList(new D(OBJ_ID_0, NAMESPACE_0, OBJ_ID_0)), //
            NAMESPACE_1, Collections.singletonList(new D(OBJ_ID_1, NAMESPACE_1, OBJ_ID_1)) //
    );

    @Override
    protected Class<EntityD> getDependencyEntityClass() {
        return EntityD.class;
    }

    @Override
    public List<Entity> getAllEntities(String namespace) {
        return preparedObjects.get(namespace).stream().map(EntityD::new).collect(Collectors.toList());
    }

    @Override
    public Entity getDependencyEntity(Map<String, Object> props) {
        String id = props.get(EntityD.__ENTITY_ID).toString();
        String namespace = props.get(EntityD.__NAMESPACE).toString();
        D d = preparedObjects.get(namespace).stream().filter(obj -> id.equals(obj.getId())).collect(Collectors.toList())
                .get(0);
        return new EntityD(d);
    }

    @Override
    public List<Relation> describeLocalEnvironment(Entity entity) {
        TestOutRelation outRelation = new TestOutRelation();
        outRelation.setFromEntity(entity);
        outRelation.setToEntityId(entity.getProps().get(EntityD.PROP_FK_B).toString());
        outRelation.setToEntity(new EntityB());
        outRelation.setNameSpace(entity.getProps().get(EntityD.__NAMESPACE).toString());
        return Collections.singletonList(outRelation);
    }
}
