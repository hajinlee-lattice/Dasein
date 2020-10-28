package com.latticeengines.telepath.testartifacts.assemblers;

import java.util.Collection;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.telepath.assemblers.Assembler;
import com.latticeengines.telepath.assemblers.BaseAssembler;
import com.latticeengines.telepath.entities.Entity;
import com.latticeengines.telepath.relations.BaseRelation;
import com.latticeengines.telepath.testartifacts.entities.EntityA;
import com.latticeengines.telepath.testartifacts.entities.EntityB;
import com.latticeengines.telepath.testartifacts.entities.EntityC;
import com.latticeengines.telepath.testartifacts.entities.EntityD;
import com.latticeengines.telepath.testartifacts.relations.TestInRelation;
import com.latticeengines.telepath.testartifacts.relations.TestOutRelation;

@Component
public class TestAssembler extends BaseAssembler implements Assembler {
    // relations:
    // A describes no relations
    // B describes {(A->B), (B->C), (D->B)}
    // C describes no relations
    // D describes {(D->B)}

    public static final String NAMESPACE_0 = "namespace0";
    public static final String NAMESPACE_1 = "namespace1";
    public static final String OBJ_ID_0 = "0";
    public static final String OBJ_ID_1 = "1";

    @Override
    public Collection<Entity> getInvolvedEntities() {
        return ImmutableSet.of(new EntityA(), new EntityB(), new EntityC(), new EntityD());
    }

    @Override
    public Set<BaseRelation> getInvolvedRelations() {
        return ImmutableSet.of(new TestInRelation(), new TestOutRelation());
    }
}
