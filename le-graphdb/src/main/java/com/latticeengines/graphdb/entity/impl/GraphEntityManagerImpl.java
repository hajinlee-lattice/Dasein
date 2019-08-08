package com.latticeengines.graphdb.entity.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.graph.EdgeCreationRequest;
import com.latticeengines.domain.exposed.graph.EdgeDeletionRequest;
import com.latticeengines.domain.exposed.graph.GraphConstants;
import com.latticeengines.domain.exposed.graph.LabelUtil;
import com.latticeengines.domain.exposed.graph.NameSpaceUtil;
import com.latticeengines.domain.exposed.graph.VertexCreationRequest;
import com.latticeengines.domain.exposed.graph.VertexDeletionRequest;
import com.latticeengines.graphdb.GraphDbUtil;
import com.latticeengines.graphdb.entity.BaseGraphEntityManager;
import com.latticeengines.graphdb.entity.GraphEntityManager;

@Component
public class GraphEntityManagerImpl extends BaseGraphEntityManagerImpl
        implements GraphEntityManager, BaseGraphEntityManager {

    private static final Logger log = LoggerFactory.getLogger(GraphEntityManagerImpl.class);

    @Inject
    private GraphDbUtil graphUtil;

    @Override
    public void addVertex(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            VertexCreationRequest request) throws Exception {
        if (StringUtils.isBlank(request.getObjectId()) || StringUtils.isBlank(request.getType())) {
            throw new RuntimeException("Make sure vertexId and Type is specified");
        }

        log.info("Received request: " + JsonUtils.serialize(request));
        if (!checkVertexExists(customerSpace, null, null, null, request.getObjectId(), request.getType())) {

            Map<String, String> propMap = new HashMap<>();
            Map<String, String> nsMap = new HashMap<>();
            String effectiveCustomerSpace = graphUtil.getNameSpaceUtil().populateNSMapAndCalculateEffectiveTenant( //
                    customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                    request.getType().trim(), nsMap);

            String newVertexId = graphUtil.getNameSpaceUtil().generateNSId( //
                    request.getObjectId().trim(), nsMap, graphUtil.getIsNSPostfix());

            String tenantLabel = graphUtil.getNameSpaceUtil().generateNSLabel(customerSpace.trim(), //
                    nsMap, graphUtil.getIsNSPostfix());
            Set<String> labels = new HashSet<>();
            if (CollectionUtils.isNotEmpty(request.getLabels())) {
                labels.addAll(request.getLabels());
            }
            labels.add(tenantLabel);
            labels.add(request.getType());
            labels.add(graphUtil.getNameSpaceUtil().generateNS(nsMap, graphUtil.getIsNSPostfix(), true));
            String label = LabelUtil.concatenateLabels(new ArrayList<>(labels));
            if (MapUtils.isNotEmpty(request.getProperties())) {
                propMap.putAll(request.getProperties());
            }
            propMap.put(GraphConstants.TENANT_ID_PROP_KEY, customerSpace);
            propMap.put(GraphConstants.OBJECT_ID_KEY, request.getObjectId());
            propMap.put(GraphConstants.OBJECT_TYPE_KEY, request.getType());
            propMap.put(GraphConstants.NS_KEY,
                    graphUtil.getNameSpaceUtil().generateNS(nsMap, graphUtil.getIsNSPostfix(), true));

            Map<String, Map<String, Map<String, String>>> outgoingEdgesToVertices = new HashMap<>();

            if (MapUtils.isNotEmpty(request.getOutgoingEdgesToVertices())) {
                Map<String, Map<String, Map<String, String>>> userSpecifiedMap = request.getOutgoingEdgesToVertices();
                userSpecifiedMap.keySet().stream() //
                        .forEach(k -> {
                            String edgeType = userSpecifiedMap.get(k).keySet().iterator().next();
                            nsMap.put(NameSpaceUtil.TYPE_KEY, edgeType);
                            String edgeLabel = graphUtil.getNameSpaceUtil().generateNS(nsMap,
                                    graphUtil.getIsNSPostfix(), false);
                            String outVertexType = request.getOutgoingVertexTypes().get(k);
                            nsMap.put(NameSpaceUtil.TYPE_KEY, outVertexType);
                            String outVertexId = graphUtil.getNameSpaceUtil().generateNSId(k, nsMap,
                                    graphUtil.getIsNSPostfix());

                            Map<String, Map<String, String>> edgePropMapWrapper = new HashMap<>();
                            Map<String, String> edgePropMap = new HashMap<>();
                            if (MapUtils.isNotEmpty(userSpecifiedMap.get(k))
                                    && MapUtils.isNotEmpty(userSpecifiedMap.get(k).get(edgeType))) {
                                edgePropMap.putAll(userSpecifiedMap.get(k).get(edgeType));
                            }
                            edgePropMap.put(GraphConstants.TENANT_ID_PROP_KEY, effectiveCustomerSpace);
                            edgePropMap.put(GraphConstants.EDGE_TYPE_KEY, edgeType.trim());
                            edgePropMap.put(GraphConstants.FROM_OID_KEY, request.getObjectId());
                            edgePropMap.put(GraphConstants.FROM_OTYPE_KEY, request.getType());
                            edgePropMap.put(GraphConstants.TO_OID_KEY, k);
                            edgePropMap.put(GraphConstants.TO_OTYPE_KEY, outVertexType);
                            edgePropMap.put(GraphConstants.NS_KEY, edgeLabel);

                            edgePropMapWrapper.put(edgeLabel, edgePropMap);
                            nsMap.put(NameSpaceUtil.TYPE_KEY, edgeType);
                            outgoingEdgesToVertices.put(//
                                    outVertexId, //
                                    edgePropMapWrapper);
                        });
            }
            nsMap.put(NameSpaceUtil.TYPE_KEY, request.getType());
            addVertex(newVertexId, label, propMap, outgoingEdgesToVertices);
        }
    }

    @Override
    public boolean addEdge(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            List<EdgeCreationRequest> requests) throws Exception {
        log.info("Received request: " + JsonUtils.serialize(requests));
        requests.stream().forEach(request -> {
            if (StringUtils.isBlank(request.getToObjectID()) //
                    || StringUtils.isBlank(request.getFromObjectID()) //
                    || StringUtils.isBlank(request.getToObjectType()) //
                    || StringUtils.isBlank(request.getFromObjectType()) //
                    || StringUtils.isBlank(request.getType())) {
                throw new RuntimeException("Make sure inVertexID, outVertexID and Type is specified");
            }

            try {
                if (!checkEdgeExists(customerSpace, overrideEnv, overrideVersion, overrideTenant, request.getType(),
                        request.getFromObjectID(), request.getFromObjectType(), request.getToObjectID(),
                        request.getToObjectType())) {
                    Map<String, String> nsMap = new HashMap<>();
                    String effectiveCustomerSpace = graphUtil.getNameSpaceUtil()
                            .populateNSMapAndCalculateEffectiveTenant(//
                                    customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                                    request.getType(), nsMap);

                    Map<String, String> propMap = new HashMap<>();
                    if (MapUtils.isNotEmpty(request.getProperties())) {
                        propMap.putAll(request.getProperties());
                    }
                    String edgeLabel = graphUtil.getNameSpaceUtil().generateNS(nsMap, graphUtil.getIsNSPostfix(),
                            false);

                    propMap.put(GraphConstants.TENANT_ID_PROP_KEY, effectiveCustomerSpace);
                    propMap.put(GraphConstants.EDGE_TYPE_KEY, request.getType().trim());
                    propMap.put(GraphConstants.FROM_OID_KEY, request.getFromObjectID().trim());
                    propMap.put(GraphConstants.FROM_OTYPE_KEY, request.getFromObjectType().trim());
                    propMap.put(GraphConstants.TO_OID_KEY, request.getToObjectID().trim());
                    propMap.put(GraphConstants.TO_OTYPE_KEY, request.getToObjectType().trim());
                    propMap.put(GraphConstants.NS_KEY, edgeLabel);

                    nsMap.put(NameSpaceUtil.TYPE_KEY, request.getToObjectType());
                    String inVertexId = graphUtil.getNameSpaceUtil().generateNSId(request.getToObjectID(), nsMap,
                            graphUtil.getIsNSPostfix());
                    nsMap.put(NameSpaceUtil.TYPE_KEY, request.getFromObjectType());
                    String outVertexId = graphUtil.getNameSpaceUtil().generateNSId(request.getFromObjectID(), nsMap,
                            graphUtil.getIsNSPostfix());
                    try {
                        addEdge(edgeLabel, propMap, inVertexId, outVertexId);
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return true;
    }

    @Override
    public boolean dropVertex(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            VertexDeletionRequest request) throws Exception {
        if (StringUtils.isBlank(request.getObjectId()) || StringUtils.isBlank(request.getType())) {
            throw new RuntimeException("Make sure vertexId and Type is specified");
        }

        log.info("Received request: " + JsonUtils.serialize(request));

        Map<String, String> nsMap = new HashMap<>();
        customerSpace = graphUtil.getNameSpaceUtil().populateNSMapAndCalculateEffectiveTenant( //
                customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                request.getType().trim(), nsMap);
        Boolean failIfDependencyExist = !Boolean.TRUE.equals(request.getForceDelete());
        return dropVertex(graphUtil.getNameSpaceUtil().generateNSId( //
                request.getObjectId().trim(), nsMap, graphUtil.getIsNSPostfix()), failIfDependencyExist);
    }

    @Override
    public boolean dropEdge(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            List<EdgeDeletionRequest> requests) throws Exception {
        requests.stream().forEach(request -> {
            if (StringUtils.isBlank(request.getToObjectID()) //
                    || StringUtils.isBlank(request.getFromObjectID()) //
                    || StringUtils.isBlank(request.getToObjectType()) //
                    || StringUtils.isBlank(request.getFromObjectType()) //
                    || StringUtils.isBlank(request.getType())) {
                throw new RuntimeException("Make sure inVertexID, outVertexID and Type is specified");
            }

            log.info("Received request: " + JsonUtils.serialize(requests));

            Map<String, String> nsMap = new HashMap<>();
            graphUtil.getNameSpaceUtil().populateNSMapAndCalculateEffectiveTenant(//
                    customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                    request.getType(), nsMap);

            String edgeLabel = graphUtil.getNameSpaceUtil().generateNS(nsMap, graphUtil.getIsNSPostfix(), false);
            nsMap.put(NameSpaceUtil.TYPE_KEY, request.getToObjectType());
            String inVertexId = graphUtil.getNameSpaceUtil().generateNSId(request.getToObjectID(), nsMap,
                    graphUtil.getIsNSPostfix());
            nsMap.put(NameSpaceUtil.TYPE_KEY, request.getFromObjectType());
            String outVertexId = graphUtil.getNameSpaceUtil().generateNSId(request.getFromObjectID(), nsMap,
                    graphUtil.getIsNSPostfix());
            try {
                dropEdge(inVertexId, outVertexId, edgeLabel);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        return true;
    }

    @Override
    public boolean updateVertexProperty(String customerSpace, String overrideEnv, String overrideVersion,
            String overrideTenant, String vertexId, Map<String, String> addOrUpdateProperties,
            List<String> removeProperties) throws Exception {
        return updateVertexProperty(vertexId, addOrUpdateProperties, removeProperties);
    }

    @Override
    public List<Map<String, String>> checkDirectVertexDependencies(String customerSpace, String overrideEnv,
            String overrideVersion, String overrideTenant, String objectId, String type) throws Exception {
        Map<String, String> nsMap = new HashMap<>();
        graphUtil.getNameSpaceUtil().populateNSMapAndCalculateEffectiveTenant( //
                customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                type.trim(), nsMap);

        String vertexId = graphUtil.getNameSpaceUtil().generateNSId( //
                objectId.trim(), nsMap, graphUtil.getIsNSPostfix());
        List<String> depList = checkDirectVertexDependencies(vertexId);
        List<Map<String, String>> result = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(depList)) {
            depList.stream() //
                    .forEach(dep -> {
                        String depObjId = graphUtil.getNameSpaceUtil().extractObjectIdFromGraphVertexId(dep,
                                graphUtil.getIsNSPostfix());
                        Map<String, String> depNsMap = graphUtil.getNameSpaceUtil().extractNsMapFromGraphVertexId(dep,
                                graphUtil.getIsNSPostfix());
                        Map<String, String> depInfo = new HashMap<>();
                        depInfo.putAll(depNsMap);
                        depInfo.put(GraphConstants.OBJECT_ID_KEY, depObjId);
                        result.add(depInfo);
                    });
        }
        return result;
    }

    @Override
    public List<List<Map<String, String>>> checkPotentialCircularDependencies(String customerSpace, String overrideEnv,
            String overrideVersion, String overrideTenant, String fromObjectId, String fromObjectType,
            String toObjectId, String toObjectType) throws Exception {

        Map<String, String> nsMap = new HashMap<>();
        graphUtil.getNameSpaceUtil() //
                .populateNSMapAndCalculateEffectiveTenant(//
                        customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                        null, nsMap);

        nsMap.put(NameSpaceUtil.TYPE_KEY, toObjectType);
        String inVertexId = graphUtil.getNameSpaceUtil().generateNSId(toObjectId, nsMap, graphUtil.getIsNSPostfix());
        nsMap.put(NameSpaceUtil.TYPE_KEY, fromObjectType);
        String outVertexId = graphUtil.getNameSpaceUtil().generateNSId(fromObjectId, nsMap, graphUtil.getIsNSPostfix());

        List<List<String>> paths = checkPotentialCircularDependencies(inVertexId, outVertexId);
        List<List<Map<String, String>>> result = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(paths)) {
            paths.stream() //
                    .forEach(dep -> {
                        List<Map<String, String>> path = new ArrayList<>();
                        result.add(path);
                        dep.stream() //
                                .forEach(p -> {
                                    String objId = graphUtil.getNameSpaceUtil().extractObjectIdFromGraphVertexId(p,
                                            graphUtil.getIsNSPostfix());
                                    Map<String, String> objNsMap = graphUtil.getNameSpaceUtil()
                                            .extractNsMapFromGraphVertexId(p, graphUtil.getIsNSPostfix());
                                    Map<String, String> vertexInfo = new HashMap<>();
                                    vertexInfo.putAll(objNsMap);
                                    vertexInfo.put(GraphConstants.OBJECT_ID_KEY, objId);
                                    path.add(vertexInfo);
                                });
                    });
        }
        return result;
    }

    @Override
    public boolean checkVertexExists(String customerSpace, String overrideEnv, String overrideVersion,
            String overrideTenant, String objectId, String objectType) throws Exception {
        if (StringUtils.isBlank(objectId) || StringUtils.isBlank(objectType)) {
            throw new RuntimeException("Make sure vertexId and Type is specified");
        }
        Map<String, String> nsMap = new HashMap<>();
        graphUtil.getNameSpaceUtil().populateNSMapAndCalculateEffectiveTenant( //
                customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                objectType.trim(), nsMap);

        String vertexId = graphUtil.getNameSpaceUtil().generateNSId( //
                objectId.trim(), nsMap, graphUtil.getIsNSPostfix());
        return checkVertexExists(vertexId);
    }

    @Override
    public boolean checkEdgeExists(String customerSpace, String overrideEnv, String overrideVersion,
            String overrideTenant, String edgeType, String fromObjectID, String fromObjectType, String toObjectID,
            String toObjectType) throws Exception {
        if (StringUtils.isBlank(edgeType) //
                || StringUtils.isBlank(fromObjectID) //
                || StringUtils.isBlank(fromObjectType) //
                || StringUtils.isBlank(toObjectID) //
                || StringUtils.isBlank(toObjectType)) {
            throw new RuntimeException("Make sure required info is passed for edge existance check");
        }
        Map<String, String> nsMap = new HashMap<>();

        graphUtil.getNameSpaceUtil().populateNSMapAndCalculateEffectiveTenant( //
                customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                fromObjectType.trim(), nsMap);

        String edgeLabel = graphUtil.getNameSpaceUtil().generateNS(nsMap, graphUtil.getIsNSPostfix(), false);

        graphUtil.getNameSpaceUtil().populateNSMapAndCalculateEffectiveTenant( //
                customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                fromObjectType.trim(), nsMap);

        String outVertexID = graphUtil.getNameSpaceUtil().generateNSId( //
                fromObjectID.trim(), nsMap, graphUtil.getIsNSPostfix());

        graphUtil.getNameSpaceUtil().populateNSMapAndCalculateEffectiveTenant( //
                customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                toObjectType.trim(), nsMap);

        String inVertexID = graphUtil.getNameSpaceUtil().generateNSId( //
                toObjectID.trim(), nsMap, graphUtil.getIsNSPostfix());

        return checkEdgeExists(edgeLabel, inVertexID, outVertexID);
    }
}
