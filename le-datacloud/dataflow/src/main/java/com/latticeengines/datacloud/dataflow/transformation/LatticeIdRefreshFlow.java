package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.LatticeIdUpdateFuction;
import com.latticeengines.domain.exposed.datacloud.dataflow.LatticeIdRefreshFlowParameter;
import com.latticeengines.domain.exposed.datacloud.manage.LatticeIdStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("latticeIdRefreshFlow")
public class LatticeIdRefreshFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, LatticeIdRefreshFlowParameter> {
    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    public final static String STATUS_FIELD = "Status";
    public final static String TIMESTAMP_FIELD = "LE_Last_Update_Date";
    public final static String REDIRECT_FROM_FIELD = "RedirectFromId";

    private final static String ENTITY = "ENTITY_";

    private static final Log log = LogFactory.getLog(LatticeIdRefreshFlow.class);

    @Override
    public Node construct(LatticeIdRefreshFlowParameter parameters) {
        LatticeIdStrategy strategy = parameters.getStrategy();
        List<String> idsKeys = getIdsKeys(strategy);
        List<String> entityKeys = getEntityKeys(strategy);

        Node ids = addSource(parameters.getBaseTables().get(0));
        Node entity = addSource(parameters.getBaseTables().get(1));
        entity = renameEntity(entity);
        entity = entity.retain(new FieldList(entityKeys));

        List<String> finalFields = new ArrayList<>();
        finalFields.addAll(ids.getFieldNames());

        Node joined = ids.join(new FieldList(idsKeys), entity, new FieldList(entityKeys), JoinType.OUTER);
        Node matched = matched(joined, idsKeys, entityKeys);
        matched = processMatched(matched, finalFields, strategy);
        Node onlyIds = onlyIds(joined, idsKeys, entityKeys);
        onlyIds = processOnlyIds(onlyIds, finalFields, strategy);
        Node onlyEntity = onlyEntity(joined, idsKeys, entityKeys);
        onlyEntity = processOnlyEntity(onlyEntity, finalFields, idsKeys, entityKeys, strategy,
                parameters.getCurrentCount());

        if (strategy.isMergeDup()) {
            // TODO: Implement merge duplicate records for CDL use case
        }

        return matched.merge(onlyIds).merge(onlyEntity);
    }

    private Node processMatched(Node node, List<String> finalFields, LatticeIdStrategy strategy) {
        node = node.retain(new FieldList(finalFields));
        node = node.apply(
                new LatticeIdUpdateFuction(
                        new Fields(node.getFieldNames().toArray(new String[node.getFieldNames().size()])), "ACTIVE",
                        STATUS_FIELD, TIMESTAMP_FIELD, null, null, null, null),
                new FieldList(node.getFieldNames()), node.getSchema(), new FieldList(node.getFieldNames()),
                Fields.REPLACE);
        return node;
    }

    private Node processOnlyIds(Node node, List<String> finalFields, LatticeIdStrategy strategy) {
        node = node.retain(new FieldList(finalFields));
        if (strategy.isCheckObsolete()) {
            node = node.apply(
                    new LatticeIdUpdateFuction(
                            new Fields(node.getFieldNames().toArray(new String[node.getFieldNames().size()])),
                            "OBSOLETE", STATUS_FIELD, TIMESTAMP_FIELD, null, null, null, null),
                    new FieldList(node.getFieldNames()), node.getSchema(), new FieldList(node.getFieldNames()),
                    Fields.REPLACE);
        }
        return node;
    }

    private Node processOnlyEntity(Node node, List<String> finalFields, List<String> idsKeys, List<String> entityKeys,
            LatticeIdStrategy strategy, Long currentCount) {
        String newId = "_NEW_" + strategy.getIdName();
        switch (strategy.getIdType()) {
        case LONG:
            node = node.addRowID(newId);
            node = node.apply(String.format("Long.valueOf(%s) + %d", newId, currentCount), new FieldList(newId),
                    new FieldMetadata(newId, Long.class));
            break;
        case UUID:
            node = node.addUUID(newId);
            break;
        default:
            throw new RuntimeException(
                    String.format("IdType %s in LatticeIdStrategy is not supported", strategy.getIdType().name()));
        }
        String copyIdFrom = newId;
        List<String> copyIdTo = new ArrayList<>();
        copyIdTo.add(REDIRECT_FROM_FIELD);
        copyIdTo.add(strategy.getIdName());
        node = node.apply(
                new LatticeIdUpdateFuction(
                        new Fields(node.getFieldNames().toArray(new String[node.getFieldNames().size()])), "ACTIVE",
                        STATUS_FIELD, TIMESTAMP_FIELD, copyIdFrom, copyIdTo, idsKeys, entityKeys),
                new FieldList(node.getFieldNames()), node.getSchema(), new FieldList(node.getFieldNames()),
                Fields.REPLACE);
        node.discard(new FieldList(newId));
        node = node.retain(new FieldList(finalFields));
        return node;
    }

    private Node matched(Node joined, List<String> idsKeys, List<String> entityKeys) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (String idsKey : idsKeys) {
            sb.append(idsKey + " != null || ");
        }
        sb.setLength(sb.length() - 4);
        sb.append(") && (");
        for (String entityKey : entityKeys) {
            sb.append(entityKey + " != null || ");
        }
        sb.setLength(sb.length() - 4);
        sb.append(")");
        List<String> filterFields = new ArrayList<>();
        filterFields.addAll(idsKeys);
        filterFields.addAll(entityKeys);
        log.info("Filter expression to find matched accounts: " + sb.toString());
        return joined.filter(sb.toString(), new FieldList(filterFields));
    }

    private Node onlyIds(Node joined, List<String> idsKeys, List<String> entityKeys) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (String idsKey : idsKeys) {
            sb.append(idsKey + " != null || ");
        }
        sb.setLength(sb.length() - 4);
        sb.append(") && (");
        for (String srcKey : entityKeys) {
            sb.append(srcKey + " == null && ");
        }
        sb.setLength(sb.length() - 4);
        sb.append(")");
        List<String> filterFields = new ArrayList<>();
        filterFields.addAll(idsKeys);
        filterFields.addAll(entityKeys);
        log.info("Filter expression to find accounts from LatticeId source: " + sb.toString());
        return joined.filter(sb.toString(), new FieldList(filterFields));
    }

    private Node onlyEntity(Node joined, List<String> idsKeys, List<String> entityKeys) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (String idsKey : idsKeys) {
            sb.append(idsKey + " == null && ");
        }
        sb.setLength(sb.length() - 4);
        sb.append(") && (");
        for (String srcKey : entityKeys) {
            sb.append(srcKey + " != null || ");
        }
        sb.setLength(sb.length() - 4);
        sb.append(")");
        List<String> filterFields = new ArrayList<>();
        filterFields.addAll(idsKeys);
        filterFields.addAll(entityKeys);
        log.info("Filter expression to find accounts from entity source: " + sb.toString());
        return joined.filter(sb.toString(), new FieldList(filterFields));
    }

    private Node renameEntity(Node src) {
        for (String attr : src.getFieldNames()) {
            src = src.rename(new FieldList(attr), new FieldList(ENTITY + attr));
        }
        return src;
    }

    private List<String> getIdsKeys(LatticeIdStrategy strategy) {
        List<String> fields = new ArrayList<>();
        for (List<String> attrs : strategy.getKeyMap().values()) {
            fields.addAll(attrs);
        }
        return fields;
    }

    private List<String> getEntityKeys(LatticeIdStrategy strategy) {
        List<String> fields = new ArrayList<>();
        for (List<String> attrs : strategy.getKeyMap().values()) {
            for (String attr : attrs) {
                fields.add(ENTITY + attr);
            }
        }
        return fields;
    }
}
