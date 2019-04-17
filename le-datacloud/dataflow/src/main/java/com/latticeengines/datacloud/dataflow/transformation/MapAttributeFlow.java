package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MapAttributeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TblDrivenFuncConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(MapAttributeFlow.BEAN_NAME)
public class MapAttributeFlow extends TblDrivenFlowBase<MapAttributeConfig, MapAttributeConfig.MapFunc> {

    private static final Logger log = LoggerFactory.getLogger(MapAttributeFlow.class);

    private static final String JOIN_KEY_PREFIX = "LDC_AM_JOINKEY_";
    private static final String JOIN_KEY_SUFFIX = "_";

    public static final String BEAN_NAME = "mapAttributeFlow";

    public static final String DERIVE_TRANSFORMER = "deriveAttribute";

    public static final String MAP_TRANSFORMER = "mapAttribute";

    public static final String MAP_STAGE = "MapStage";
    public static final String DERIVE_STAGE = "DeriveStage";
    public static final String REFRESH_STAGE = "RefreshStage";
  
    int joinKeyIdx = 0;

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        MapAttributeConfig config = getTransformerConfig(parameters);

        Map<String, Node> sourceMap = initiateSourceMap(parameters, config);

        List<MapAttributeConfig.MapFunc> attributes = getAttributeFuncs(config);

        if ((attributes == null)  || (sourceMap == null)) {
            throw new RuntimeException("Invalid configuration");
        }

        Map<String, List<MapAttributeConfig.MapFunc>> sourceAttributeMap = prepMapsPerSource(attributes);

        String seedName = config.getSeed();
        log.info("Prepare seed " + seedName);
        Node seed = null;
        if (config.getStage().equals(REFRESH_STAGE)) {
            List<SourceAttribute> sourceAttributes = getAttributes(config);
            seed = sourceMap.get(seedName);
            if (seed == null) {
                throw new RuntimeException("Failed to prepare seed " + seedName);
            }
            seed = discardExistingAttrs(seed, seedName, sourceAttributes, parameters.getTimestampField());
        } else {
            seed = prepareSource(sourceMap.get(seedName), sourceAttributeMap.get(seedName), null, null);
            if (seed == null) {
                throw new RuntimeException("Failed to prepare seed " + seedName);
            }
        }

        List<MapAttributeConfig.JoinConfig> joinConfigs = config.getJoinConfigs();
        String seedId = config.getSeedId();

        Node joined = seed;

        Map<Node, String> joinKeyMap = new HashMap<Node, String>();
        for (MapAttributeConfig.JoinConfig joinConfig : joinConfigs) {

             List<String> seedJoinFields = joinConfig.getKeys();
             
             List<Node> sources = new ArrayList<Node>();

             for (MapAttributeConfig.JoinTarget joinTarget : joinConfig.getTargets()) {
                 String sourceName = joinTarget.getSource();
                 Node source = sourceMap.get(sourceName);
                 if (source == null) {
                    throw new RuntimeException("Failed to prepare sourcce " + sourceName);
                 }
                source = prepareSource(source, sourceAttributeMap.get(sourceName), joinTarget.getKeys(), joinKeyMap);
                 sources.add(source);
             }

             log.info("Converging sources");

             joined = convergeSources(joined, seed, sources, seedId, seedJoinFields, joinKeyMap);
             log.info("Converged " + sources.size() + " sources");
 
        }

        FieldList joinFields = getJoinFields(joined);
        Node discarded = joined.discard(joinFields);
        Node stamped = discarded.addTimestamp(parameters.getTimestampField());

        return stamped;
    }

    private Node discardExistingAttrs(Node node, String sourceName, List<SourceAttribute> srcAttrs, String timestampField) {
        Set<String> existAttrs = new HashSet<>(node.getFieldNames());
        List<String> discardAttrs = new ArrayList<>();
        for (SourceAttribute srcAttr : srcAttrs) {
            if (existAttrs.contains(srcAttr.getAttribute())) {
                discardAttrs.add(srcAttr.getAttribute());
            } else {
                log.info(String.format("Attribute %s does not exist in source %s", srcAttr.getAttribute(), sourceName));
            }
        }
        if (StringUtils.isNotBlank(timestampField) && existAttrs.contains(timestampField)) {
            discardAttrs.add(timestampField);
        }
        return node.discard(new FieldList(discardAttrs));
    }

    private Node prepareSource(Node source, List<MapAttributeConfig.MapFunc> mapConfigs, List<String> joinKeys, Map<Node, String> joinKeyMap) {

        if (joinKeys != null) {
            String[] joinKeyArray = joinKeys.toArray(new String[joinKeys.size()]);
            source = source.groupByAndLimit(new FieldList(joinKeyArray), 1);
        }

        List<String> columns = source.getFieldNames();
        Set<String> columnSet = new HashSet<String>(columns);


        List<String> sourceAttrs = new ArrayList<String>();
        List<String> outputAttrs = new ArrayList<String>();

        // Filter out missing attributes and join keys
        for (MapAttributeConfig.MapFunc mapConfig : mapConfigs) {
             String srcAttr = mapConfig.getAttribute();
             if (!columnSet.contains(srcAttr)) {
                 log.info("Src Attr " + srcAttr + " does not exist in source");
                 continue;
             }
             if (joinKeys != null) {
                 boolean found = false;
                 for (String key : joinKeys) {
                     if (key.equals(srcAttr)) {
                         log.info("Join key " + key + " should not be in map");
                         found = true;
                     }
                  }
                  if (found) {
                      continue;
                  }
             } 
             sourceAttrs.add(srcAttr);
             outputAttrs.add(mapConfig.getTarget());
        }

        String outputJoinKey = null;
        if (joinKeys != null) {
            outputJoinKey = newJoinKey();
            sourceAttrs.add(joinKeys.get(0));
            outputAttrs.add(outputJoinKey);

            for (int i = 1; i < joinKeys.size(); i++) {
                String outputKey = outputJoinKey + JOIN_KEY_SUFFIX + i;
                sourceAttrs.add(joinKeys.get(i));
                outputAttrs.add(outputKey);
            }
        }

        Node renamed = source.rename(new FieldList(sourceAttrs), new FieldList(outputAttrs));
        Node retained = renamed.retain(new FieldList(outputAttrs));

        if (outputJoinKey != null) {
            joinKeyMap.put(retained, outputJoinKey);
        }

        return retained;
    }


    private Map<String, List<MapAttributeConfig.MapFunc>> prepMapsPerSource(List<MapAttributeConfig.MapFunc> mapFuncs) {
        Map<String, List<MapAttributeConfig.MapFunc>> sourceAttributes = new HashMap<String, List<MapAttributeConfig.MapFunc>>();
        for (MapAttributeConfig.MapFunc mapFunc : mapFuncs) {
            String origSource = mapFunc.getSource();
            List<MapAttributeConfig.MapFunc> attrsPerSrc = sourceAttributes.get(origSource);
            if (attrsPerSrc == null) {
                attrsPerSrc = new ArrayList<MapAttributeConfig.MapFunc>();
                sourceAttributes.put(origSource, attrsPerSrc);
            }
            attrsPerSrc.add(mapFunc);
        } 
        return sourceAttributes;
    } 

    private Node convergeSources(Node joined, Node seed, List<Node> nodes, String seedId, List<String> seedJoinKey, Map<Node, String> joinKeyMap) {

        if (nodes.size() == 0) {
            log.info("No source for " + seedJoinKey.get(0));
            return null;
        }

        List<FieldList> joinFieldLists = new ArrayList<FieldList>();
        for (Node node : nodes) {
            String[] joinKeys = new String[seedJoinKey.size()];
            joinKeys[0] = joinKeyMap.get(node);
            for (int i = 1; i < seedJoinKey.size(); i++) {
                joinKeys[i] = joinKeys[0] + JOIN_KEY_SUFFIX + i;
            }
            joinFieldLists.add(new FieldList(joinKeys));
        }

        String[] seedAttrs = new String[seedJoinKey.size() + 1];
        String[] seedOutputAttrs = new String[seedJoinKey.size() + 1];
        String[] seedJoinAttrs = new String[seedJoinKey.size()];

        String seedOutputId = newJoinKey();
        seedAttrs[0] = seedId;
        seedOutputAttrs[0] = seedOutputId;

        String filterString = "((" + seedJoinKey.get(0) + " != null)";
        seedAttrs[1] = seedJoinKey.get(0);
        seedOutputAttrs[1] = seedOutputId + JOIN_KEY_SUFFIX + 1;
        seedJoinAttrs[0] = seedOutputAttrs[1];
        for (int i = 1; i < seedJoinKey.size(); i++) {
            filterString += "|| (" + seedJoinKey.get(i) + " != null)";
            seedAttrs[i + 1] = seedJoinKey.get(i);
            seedOutputAttrs[i + 1] = seedOutputId + JOIN_KEY_SUFFIX + (i + 1);
            seedJoinAttrs[i] = seedOutputAttrs[i + 1];
        }
        filterString += ")";

        FieldList seedFields = new FieldList(seedAttrs);
        Node filteredSeed = seed.filter(filterString, seedFields);

        FieldList seedOutputFields = new FieldList(seedOutputAttrs);
        Node renamedSeed = filteredSeed.rename(seedFields, seedOutputFields);
        Node retainedSeed = renamedSeed.retain(seedOutputFields);

        Node coGrouped = retainedSeed.coGroup(new FieldList(seedJoinAttrs), nodes, joinFieldLists, JoinType.OUTER);

        Node filtered = coGrouped.filter(seedOutputId + " != null", new FieldList(seedOutputId));
        joined = joined.leftJoin(new FieldList(seedId), filtered, new FieldList(seedOutputId));

        return joined;

    }


    private String newJoinKey() {
        String joinKey = JOIN_KEY_PREFIX + joinKeyIdx;
        joinKeyIdx++;
        log.info("New Join Key " + joinKey);
        return joinKey;
    }

    private FieldList getJoinFields(Node node) {
        List<String> fieldNames = node.getFieldNames();
        List<String> joinList = new ArrayList<String>();
        for (String field : fieldNames) {
            if (field.startsWith(JOIN_KEY_PREFIX)) {
                joinList.add(field);
            }
        }
        return new FieldList(joinList);
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return MapAttributeConfig.class;
    }

    @Override
    public Class<? extends TblDrivenFuncConfig> getTblDrivenFuncConfigClass() {
        return MapAttributeConfig.MapFunc.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "mapAttributeFlow";
    }

    @Override
    public String getTransformerName() {
        return "mapAttribute";

    }
}
