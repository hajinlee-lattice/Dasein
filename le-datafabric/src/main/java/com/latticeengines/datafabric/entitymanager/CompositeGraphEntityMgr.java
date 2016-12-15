package com.latticeengines.datafabric.entitymanager;

import java.util.List;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.domain.exposed.datafabric.CompositeFabricEntity;
import com.latticeengines.domain.exposed.datafabric.CompositeGraphEntity;

public interface CompositeGraphEntityMgr extends GraphNode {

    String getName();
    CompositeFabricEntityMgr<? extends CompositeFabricEntity> getManager();
    CompositeGraphEntityMgr getChild(String name);
    void addChild(CompositeGraphEntityMgr child);
    CompositeFabricEntityMgr<? extends CompositeFabricEntity> getEntityMgr(List<String> path);
    CompositeGraphEntityMgr getGraphMgr(List<String> path);
    CompositeFabricEntity findEntity(List<String> Path, String id);
    List<CompositeGraphEntity> findChildren(List<String> Path, String id);
    List<CompositeGraphEntity> findChildren(String id);
    CompositeGraphEntity find(List<String> Path, String id) ;
    CompositeGraphEntity find(String id);
    void delete(List<String> path, String id);
    void deleteChildren(List<String> path, String id);
    void delete(String id);
    void deleteChildren(String id);
}
