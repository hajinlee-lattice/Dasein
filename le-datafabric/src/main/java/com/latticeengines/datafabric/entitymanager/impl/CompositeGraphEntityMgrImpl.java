package com.latticeengines.datafabric.entitymanager.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.datafabric.entitymanager.CompositeFabricEntityMgr;
import com.latticeengines.datafabric.entitymanager.CompositeGraphEntityMgr;
import com.latticeengines.domain.exposed.datafabric.CompositeFabricEntity;
import com.latticeengines.domain.exposed.datafabric.CompositeGraphEntity;

public class CompositeGraphEntityMgrImpl implements CompositeGraphEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(CompositeGraphEntityMgrImpl.class);

    private final int FIND = 0;
    private final int DELETE = 1;

    private String name;
    private CompositeFabricEntityMgr<? extends CompositeFabricEntity> manager;
    private List<CompositeGraphEntityMgr> children;
    private Map<String, CompositeGraphEntityMgr> childMap;

    CompositeGraphEntityMgrImpl(String name, CompositeFabricEntityMgr<? extends CompositeFabricEntity> manager) {
        this.name = name;
        this.manager = manager;
        childMap = new HashMap<String, CompositeGraphEntityMgr>();
        children = new ArrayList<CompositeGraphEntityMgr>();
    }

    public String getName() {
        return name;
    }

    public CompositeFabricEntityMgr<? extends CompositeFabricEntity> getManager() {
        return manager;
    }

    public Collection<CompositeGraphEntityMgr> getChildren() {
        return children;
    }

    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return null;
    }

    public CompositeGraphEntityMgr getChild(String name) {
        return childMap.get(name);
    }

    public void addChild(CompositeGraphEntityMgr child) {
        children.add(child);
        CompositeGraphEntityMgr mgr = childMap.get(child.getName());
        if (mgr != null) {
            log.info("Only allows one child per name");
        } else {
            childMap.put(child.getName(), child);
        }
    }

    public CompositeFabricEntityMgr<? extends CompositeFabricEntity> getEntityMgr(List<String> path) {
        CompositeGraphEntityMgr graphMgr = getGraphMgr(path);
        if (graphMgr == null) {
            return null;
        } else {
            return graphMgr.getManager();
        }

    }

    public CompositeGraphEntityMgr getGraphMgr(List<String> path) {
        CompositeGraphEntityMgr graphMgr = getChild(path.get(0));
        if ((graphMgr != null) && (path.size() > 1)) {
            List<String> nextPath = new ArrayList<String>();
            for (int i = 1; i < path.size(); i++) {
                nextPath.add(path.get(i));
            }
            graphMgr = graphMgr.getGraphMgr(nextPath);
        }
        return graphMgr;
    }

    public CompositeFabricEntity findEntity(List<String> path, String id) {
        CompositeFabricEntityMgr<? extends CompositeFabricEntity> mgr = getEntityMgr(path);
        return mgr.findByKey(id);
    }

    public List<CompositeGraphEntity> findChildren(List<String> path, String id) {
        CompositeGraphEntityMgr mgr = getGraphMgr(path);
        return mgr.findChildren(id);
    }

    public List<CompositeGraphEntity> findChildren(String id) {
        GraphCollector collector = new GraphCollector();
        collector.addParentId(this, id);
        collectEntities(collector);
        List<CompositeGraphEntity> entities = collector.getEntities(this);
        return entities;
    }

    public CompositeGraphEntity find(List<String> path, String id) {
        CompositeGraphEntityMgr mgr = getGraphMgr(path);
        return mgr.find(id);
    }

    public CompositeGraphEntity find(String id) {
        GraphCollector collector = new GraphCollector();
        collector.setId(this, id);
        collectEntities(collector);
        List<CompositeGraphEntity> entities = collector.getEntities(this);
        if ((entities == null) || (entities.size() == 0)) {
            return null;
        } else {
            return entities.get(0);
        }
    }

    private void collectEntities(GraphCollector collector) {
        collector.setMode(FIND);
        BreadthFirstSearch searchAlg = new BreadthFirstSearch();
        searchAlg.run(this, collector);
    }

    public void delete(List<String> path, String id) {
        CompositeGraphEntityMgr mgr = getGraphMgr(path);
        mgr.delete(id);
    }

    public void deleteChildren(List<String> path, String id) {
        CompositeGraphEntityMgr mgr = getGraphMgr(path);
        mgr.deleteChildren(id);
    }

    public void delete(String id) {
        GraphCollector collector = new GraphCollector();
        collector.setId(this, id);
        collectEntities(collector);
        collector.setMode(DELETE);
        DepthFirstSearch searchAlg = new DepthFirstSearch();
        searchAlg.run(this, collector, true);
    }

    public void deleteChildren(String id) {
        GraphCollector collector = new GraphCollector();
        collector.addParentId(this, id);
        collectEntities(collector);
        collector.setMode(DELETE);
        DepthFirstSearch searchAlg = new DepthFirstSearch();
        searchAlg.run(this, collector, true);
    }

    private void addToCollector(GraphCollector collector, CompositeFabricEntity entity) {
        CompositeGraphEntity graphEntity = new CompositeGraphEntity(name, entity);
        collector.addEntity(this, graphEntity);
        for (CompositeGraphEntityMgr mgr : children) {
            collector.addParentId(mgr, entity.getEntityId());
        }
    }

    public void accept(Visitor visitor, VisitorContext context) {

        GraphCollector collector = (GraphCollector) visitor;
        List<String> parentIds = collector.getParentIds(this);
        switch (collector.getMode()) {
        case FIND:
            if (parentIds == null) {
                String id = collector.getId(this);
                if (id != null) {
                    CompositeFabricEntity entity = manager.findByKey(id);
                    if (entity != null) {
                        addToCollector(collector, entity);
                    }
                }
            } else {
                for (String parentId : parentIds) {
                    List<? extends CompositeFabricEntity> entities = manager.findChildren(parentId, name);
                    if (entities != null) {
                        for (CompositeFabricEntity entity : entities) {
                            addToCollector(collector, entity);
                        }
                    }
                }
            }
            break;
        case DELETE:
            if (parentIds != null) {
                for (String parentId : parentIds) {
                    manager.deleteChildren(name, parentId);
                }
            } else {
                String id = collector.getId(this);
                if (id != null) {
                    manager.deleteByKey(id);
                }
            }
            break;
        }
    }

    class GraphCollector implements Visitor {
        int mode;
        Map<CompositeGraphEntityMgr, List<String>> parentMap;
        Map<String, CompositeGraphEntity> entityMap;
        Map<CompositeGraphEntityMgr, List<CompositeGraphEntity>> entityMgrMap;
        Map<CompositeGraphEntityMgr, String> idMap;

        GraphCollector() {
            parentMap = new HashMap<CompositeGraphEntityMgr, List<String>>();
            entityMap = new HashMap<String, CompositeGraphEntity>();
            entityMgrMap = new HashMap<CompositeGraphEntityMgr, List<CompositeGraphEntity>>();
            idMap = new HashMap<CompositeGraphEntityMgr, String>();
        }

        public int getMode() {
            return mode;
        }

        public void setMode(int mode) {
            this.mode = mode;
        }

        public List<String> getParentIds(CompositeGraphEntityMgr mgr) {
            return parentMap.get(mgr);
        }

        public void addEntity(CompositeGraphEntityMgr mgr, CompositeGraphEntity entity) {
            String id = entity.getEntity().getEntityId();
            entityMap.put(id, entity);
            String parentId = entity.getEntity().getParentId();
            CompositeGraphEntity parent = entityMap.get(parentId);
            if (parent != null) {
                parent.addChild(entity);
            }
            List<CompositeGraphEntity> entities = entityMgrMap.get(mgr);
            if (entities == null) {
                entities = new ArrayList<CompositeGraphEntity>();
                entityMgrMap.put(mgr, entities);
            }
            entities.add(entity);
        }

        public void addParentId(CompositeGraphEntityMgr mgr, String id) {
            List<String> ids = parentMap.get(mgr);
            if (ids == null) {
                ids = new ArrayList<String>();
                parentMap.put(mgr, ids);
            }
            ids.add(id);
        }

        public void setId(CompositeGraphEntityMgr mgr, String id) {
            idMap.put(mgr, id);
        }

        public String getId(CompositeGraphEntityMgr mgr) {
            return idMap.get(mgr);
        }

        public CompositeGraphEntity getEntity(String id) {
            return entityMap.get(id);
        }

        public List<CompositeGraphEntity> getEntities(CompositeGraphEntityMgr mgr) {
            return entityMgrMap.get(mgr);
        }

        public void visit(Object o, VisitorContext ctx) {
        }
    }
}
