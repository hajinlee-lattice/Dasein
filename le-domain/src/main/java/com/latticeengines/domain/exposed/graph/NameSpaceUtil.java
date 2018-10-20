package com.latticeengines.domain.exposed.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

public class NameSpaceUtil {

    private static final String NULL = "^NULL^";
    private static final String NS_VAL_END = "]]";
    private static final String NS_VAL_START = "[[";
    private static final String NS_TYPE_INDICATOR = "T";
    private static final String NS_VERSION_INDICATOR = "V";
    private static final String NS_ENV_INDICATOR = "E";
    private static final String NS_PART_SEP = "*";
    private static final String NS_ID_SEP = "_^_";
    public static String ENV_KEY = "ENV";
    public static String VERSION_KEY = "VER";
    public static String TYPE_KEY = "TYPE";

    private String defaultEnv;

    private String defaultVersion;

    public NameSpaceUtil(String defaultEnv, String defaultVersion) {
        this.defaultEnv = defaultEnv;
        this.defaultVersion = defaultVersion;
    }

    public String generateNS(Map<String, String> nsMap, boolean isNSPostfix,
            boolean withoutTypeNS) {
        String env = null;
        String version = null;
        String type = null;

        if (MapUtils.isNotEmpty(nsMap)) {
            env = nsMap.get(ENV_KEY);
            version = nsMap.get(VERSION_KEY);
            type = nsMap.get(TYPE_KEY);
        }

        StringBuilder sb = new StringBuilder();
        if (isNSPostfix) {
            sb.append(NS_ID_SEP);
        }
        sb.append(NS_ENV_INDICATOR).append(NS_VAL_START) //
                .append(replaceNullWithNullMarker(env)).append(NS_VAL_END);
        sb.append(NS_PART_SEP);
        sb.append(NS_VERSION_INDICATOR).append(NS_VAL_START) //
                .append(replaceNullWithNullMarker(version)).append(NS_VAL_END);
        if (!withoutTypeNS) {
            sb.append(NS_PART_SEP);
            sb.append(NS_TYPE_INDICATOR).append(NS_VAL_START) //
                    .append(replaceNullWithNullMarker(type)).append(NS_VAL_END);
        }
        if (!isNSPostfix) {
            sb.append(NS_ID_SEP);
        }
        return sb.toString();
    }

    private String replaceNullWithNullMarker(String nsVal) {
        return nsVal == null ? NULL : nsVal;
    }

    private String handleNullMarker(String val) {
        return NULL.equals(val) ? NULL : val;
    }

    public String generateNSId(String id, Map<String, String> nsMap, boolean isNSPostfix) {
        String ns = generateNS(nsMap, isNSPostfix, false);
        return String.format("%s%s", isNSPostfix ? id : ns, isNSPostfix ? ns : id);
    }

    public String generateNSLabel(String label, Map<String, String> nsMap, boolean isNSPostfix) {
        String ns = generateNS(nsMap, isNSPostfix, true);
        return String.format("%s%s", isNSPostfix ? label : ns, isNSPostfix ? ns : label);
    }

    public Map<String, String> extractNsMapFromGraphVertexId(String graphVertexId,
            boolean isNSPostfix) {
        Map<String, String> nsMap = new HashMap<>();
        if (StringUtils.isNotBlank(graphVertexId) && graphVertexId.contains(NS_ID_SEP)) {
            String nsPart = null;
            if (isNSPostfix) {
                nsPart = graphVertexId.substring( //
                        graphVertexId.lastIndexOf(NS_ID_SEP) //
                                + NS_ID_SEP.length());
            } else {
                nsPart = graphVertexId.substring(0, //
                        graphVertexId.indexOf(NS_ID_SEP));
            }

            if (StringUtils.isNotBlank(nsPart)//
                    && nsPart.contains(NS_VAL_START) //
                    && nsPart.contains(NS_VAL_END)) {
                StringTokenizer st = new StringTokenizer(nsPart.trim(), NS_PART_SEP);
                while (st.hasMoreTokens()) {
                    String token = st.nextToken();
                    if (StringUtils.isNotBlank(token)//
                            && token.contains(NS_VAL_START) //
                            && token.contains(NS_VAL_END)) {
                        String val = token.substring(
                                token.indexOf(NS_VAL_START) + NS_VAL_START.length(), //
                                token.lastIndexOf(NS_VAL_END));
                        val = handleNullMarker(val);

                        String key = null;
                        String indicator = token.substring(0, token.indexOf(NS_VAL_START));
                        if (NS_ENV_INDICATOR.equals(indicator)) {
                            key = ENV_KEY;
                        } else if (NS_VERSION_INDICATOR.equals(indicator)) {
                            key = VERSION_KEY;
                        } else if (NS_TYPE_INDICATOR.equals(indicator)) {
                            key = TYPE_KEY;
                        }

                        if (StringUtils.isNotBlank(key)) {
                            nsMap.put(key, val);
                        }
                    }
                }
            }
        }
        return nsMap;
    }

    public String extractObjectIdFromGraphVertexId(String graphVertexId, boolean isNSPostfix) {
        String objectId = graphVertexId;
        if (StringUtils.isNotBlank(graphVertexId) && graphVertexId.contains(NS_ID_SEP)) {
            if (isNSPostfix) {
                objectId = graphVertexId.substring(0, //
                        graphVertexId.indexOf(NS_ID_SEP));
            } else {
                objectId = graphVertexId.substring( //
                        graphVertexId.lastIndexOf(NS_ID_SEP) //
                                + NS_ID_SEP.length());
            }
        }
        return objectId;
    }

    public String populateNSMapAndCalculateEffectiveTenant(String customerSpace, String overrideEnv,
            String overrideVersion, String overrideTenant, String type, Map<String, String> nsMap) {
        if (StringUtils.isNotEmpty(overrideTenant)) {
            customerSpace = overrideTenant;
        }
        if (StringUtils.isNotBlank(overrideEnv)) {
            nsMap.put(NameSpaceUtil.ENV_KEY, overrideEnv.trim());
        } else {
            nsMap.put(NameSpaceUtil.ENV_KEY, defaultEnv);
        }

        if (StringUtils.isNotBlank(overrideVersion)) {
            nsMap.put(NameSpaceUtil.VERSION_KEY, overrideVersion.trim());
        } else {
            nsMap.put(NameSpaceUtil.VERSION_KEY, defaultVersion);
        }

        nsMap.put(NameSpaceUtil.TYPE_KEY, type);
        return customerSpace;
    }
}
