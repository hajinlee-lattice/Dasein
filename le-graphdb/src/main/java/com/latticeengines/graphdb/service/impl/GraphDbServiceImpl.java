package com.latticeengines.graphdb.service.impl;

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
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.graph.EdgeCreationRequest;
import com.latticeengines.domain.exposed.graph.EdgeDeletionRequest;
import com.latticeengines.domain.exposed.graph.GraphConstants;
import com.latticeengines.domain.exposed.graph.LabelUtil;
import com.latticeengines.domain.exposed.graph.NameSpaceUtil;
import com.latticeengines.domain.exposed.graph.VertexCreationRequest;
import com.latticeengines.domain.exposed.graph.VertexDeletionRequest;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.graphdb.GraphDbUtil;
import com.latticeengines.graphdb.entity.BaseGraphEntityManager;
import com.latticeengines.graphdb.service.GraphDbService;

@Component
public class GraphDbServiceImpl implements GraphDbService {

    @Inject
    private BaseGraphEntityManager graphEntityManager;

    @Inject
    private GraphDbUtil graphUtil;

    @Override
    public boolean addVertex(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            VertexCreationRequest request) throws Exception {
        if (StringUtils.isBlank(request.getObjectId()) || StringUtils.isBlank(request.getType())) {
            throw new RuntimeException("Make sure vertexId and Type is specified");
        }

        Map<String, String> propMap = new HashMap<>();
        Map<String, String> nsMap = new HashMap<>();
        String effectiveCustomerSpace = graphUtil.getNameSpaceUtil()
                .populateNSMapAndCalculateEffectiveTenant( //
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
        labels.add(graphUtil.getNameSpaceUtil().generateNS(nsMap,
                graphUtil.getIsNSPostfix(), true));
        String label = LabelUtil.concatenateLabels(new ArrayList<>(labels));
        if (MapUtils.isNotEmpty(request.getProperties())) {
            propMap.putAll(request.getProperties());
        }
        propMap.put(GraphConstants.TENANT_ID_PROP_KEY, customerSpace);
        propMap.put(GraphConstants.OBJECT_ID_KEY, request.getObjectId());
        propMap.put(GraphConstants.OBJECT_TYPE_KEY, request.getType());
        propMap.put(GraphConstants.NS_KEY, graphUtil.getNameSpaceUtil().generateNS(nsMap,
                graphUtil.getIsNSPostfix(), true));

        if (request.getType().trim().equals(VertexType.RATING_ATTRIBUTE)
                || request.getType().trim().equals(VertexType.RATING_SCORE_ATTRIBUTE)
                || request.getType().trim().equals(VertexType.RATING_EV_ATTRIBUTE)) {
            if (!propMap.containsKey(GraphConstants.BEHAVIOR_ON_DELETE_OF_IN_VERTEX_KEY)) {
                propMap.put(GraphConstants.BEHAVIOR_ON_DELETE_OF_IN_VERTEX_KEY, //
                        GraphConstants.CASCADE_ON_DELETE);
            }
            if (!propMap.containsKey(GraphConstants.BEHAVIOR_ON_DEP_CHECK_TRAVERSAL_KEY)) {
                propMap.put(GraphConstants.BEHAVIOR_ON_DEP_CHECK_TRAVERSAL_KEY, //
                        GraphConstants.JUMP_DURING_DEP_CHECK);
            }
        }
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
                        if (MapUtils.isNotEmpty(userSpecifiedMap.get(k))) {
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
        return graphEntityManager.addVertex( //
                newVertexId, label, propMap, outgoingEdgesToVertices);
    }

    @Override
    public boolean addEdge(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            List<EdgeCreationRequest> requests) throws Exception {
        requests.stream().forEach(request -> {
            if (StringUtils.isBlank(request.getToObjectID()) //
                    || StringUtils.isBlank(request.getFromObjectID()) //
                    || StringUtils.isBlank(request.getToObjectType()) //
                    || StringUtils.isBlank(request.getFromObjectType()) //
                    || StringUtils.isBlank(request.getType())) {
                throw new RuntimeException("Make sure inVertexID, outVertexID and Type is specified");
            }

            Map<String, String> nsMap = new HashMap<>();
            String effectiveCustomerSpace = graphUtil.getNameSpaceUtil()
                    .populateNSMapAndCalculateEffectiveTenant(//
                            customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                            request.getType(), nsMap);

            Map<String, String> propMap = new HashMap<>();
            if (MapUtils.isNotEmpty(request.getProperties())) {
                propMap.putAll(request.getProperties());
            }
            String edgeLabel = graphUtil.getNameSpaceUtil().generateNS(nsMap,
                    graphUtil.getIsNSPostfix(), false);

            propMap.put(GraphConstants.TENANT_ID_PROP_KEY, effectiveCustomerSpace);
            propMap.put(GraphConstants.EDGE_TYPE_KEY, request.getType().trim());
            propMap.put(GraphConstants.FROM_OID_KEY, request.getFromObjectID().trim());
            propMap.put(GraphConstants.FROM_OTYPE_KEY, request.getFromObjectType().trim());
            propMap.put(GraphConstants.TO_OID_KEY, request.getToObjectID().trim());
            propMap.put(GraphConstants.TO_OTYPE_KEY, request.getToObjectType().trim());
            propMap.put(GraphConstants.NS_KEY, edgeLabel);

            nsMap.put(NameSpaceUtil.TYPE_KEY, request.getToObjectType());
            String inVertexId = graphUtil.getNameSpaceUtil().generateNSId(request.getToObjectID(),
                    nsMap, graphUtil.getIsNSPostfix());
            nsMap.put(NameSpaceUtil.TYPE_KEY, request.getFromObjectType());
            String outVertexId = graphUtil.getNameSpaceUtil().generateNSId(request.getFromObjectID(),
                    nsMap, graphUtil.getIsNSPostfix());
            try {
                graphEntityManager.addEdge(edgeLabel, propMap, inVertexId, outVertexId);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
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

        Map<String, String> nsMap = new HashMap<>();
        customerSpace = graphUtil.getNameSpaceUtil().populateNSMapAndCalculateEffectiveTenant( //
                customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                request.getType().trim(), nsMap);
        Boolean failIfDependencyExist = !Boolean.TRUE.equals(request.getForceDelete());
        return graphEntityManager.dropVertex(
                graphUtil.getNameSpaceUtil().generateNSId( //
                        request.getObjectId().trim(), nsMap, graphUtil.getIsNSPostfix()),
                failIfDependencyExist);
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

            Map<String, String> nsMap = new HashMap<>();
            graphUtil.getNameSpaceUtil().populateNSMapAndCalculateEffectiveTenant(//
                    customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                    request.getType(), nsMap);

            String edgeLabel = graphUtil.getNameSpaceUtil().generateNS(nsMap,
                    graphUtil.getIsNSPostfix(), false);
            nsMap.put(NameSpaceUtil.TYPE_KEY, request.getToObjectType());
            String inVertexId = graphUtil.getNameSpaceUtil().generateNSId(request.getToObjectID(),
                    nsMap, graphUtil.getIsNSPostfix());
            nsMap.put(NameSpaceUtil.TYPE_KEY, request.getFromObjectType());
            String outVertexId = graphUtil.getNameSpaceUtil().generateNSId(request.getFromObjectID(),
                    nsMap, graphUtil.getIsNSPostfix());
            try {
                graphEntityManager.dropEdge(inVertexId, outVertexId, edgeLabel);
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
        return graphEntityManager.updateVertexProperty(vertexId, addOrUpdateProperties, removeProperties);
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
        List<String> depList = graphEntityManager.checkDirectVertexDependencies(vertexId);
        List<Map<String, String>> result = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(depList)) {
            depList.stream() //
                    .forEach(dep -> {
                        String depObjId = graphUtil.getNameSpaceUtil()
                                .extractObjectIdFromGraphVertexId(dep, graphUtil.getIsNSPostfix());
                        Map<String, String> depNsMap = graphUtil.getNameSpaceUtil()
                                .extractNsMapFromGraphVertexId(dep, graphUtil.getIsNSPostfix());
                        Map<String, String> depInfo = new HashMap<>();
                        depInfo.putAll(depNsMap);
                        depInfo.put(GraphConstants.OBJECT_ID_KEY, depObjId);
                        result.add(depInfo);
                    });
        }
        return result;
    }

    @Override
    public List<Map<String, String>> checkDeepVertexDependencies(String customerSpace, String overrideEnv,
            String overrideVersion, String overrideTenant, String objectId, String type) throws Exception {
        Map<String, String> nsMap = new HashMap<>();
        graphUtil.getNameSpaceUtil().populateNSMapAndCalculateEffectiveTenant( //
                customerSpace, overrideEnv, overrideVersion, overrideTenant, //
                type.trim(), nsMap);

        String vertexId = graphUtil.getNameSpaceUtil().generateNSId( //
                objectId.trim(), nsMap, graphUtil.getIsNSPostfix());
        List<String> depList = graphEntityManager.checkDirectVertexDependencies(vertexId);
        List<Map<String, String>> result = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(depList)) {
            depList.stream() //
                    .forEach(dep -> {
                        String depObjId = graphUtil.getNameSpaceUtil()
                                .extractObjectIdFromGraphVertexId(dep, graphUtil.getIsNSPostfix());
                        Map<String, String> depNsMap = graphUtil.getNameSpaceUtil()
                                .extractNsMapFromGraphVertexId(dep, graphUtil.getIsNSPostfix());
                        Map<String, String> depInfo = new HashMap<>();
                        depInfo.putAll(depNsMap);
                        depInfo.put(GraphConstants.OBJECT_ID_KEY, depObjId);
                        result.add(depInfo);
                    });
        }
        return result;
    }
}
