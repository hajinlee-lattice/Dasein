package com.latticeengines.telepath.tools;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.telepath.entities.Entity;
import com.latticeengines.telepath.relations.Relation;

public class ImpactKernel {
    public final List<Entity> entitiesToCreate = new ArrayList<>();
    public final List<Relation> relationsToCreate = new ArrayList<>();
    public final List<Entity> entitiesToDelete = new ArrayList<>();
    public final List<Relation> relationsToDelete = new ArrayList<>();
}
