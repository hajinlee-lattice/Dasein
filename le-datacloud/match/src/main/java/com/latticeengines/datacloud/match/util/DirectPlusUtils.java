package com.latticeengines.datacloud.match.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchDataProfile;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchGrade;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchInsight;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.config.ExclusionCriterion;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public final class DirectPlusUtils {

    private static final Logger log = LoggerFactory.getLogger(DirectPlusUtils.class);

    protected DirectPlusUtils() {
        throw new UnsupportedOperationException();
    }

    public static String constructUrlParams(DnBMatchContext context, DnBAPIType apiType) {
        List<String> parts = new ArrayList<>();
        switch (apiType) {
            case REALTIME_ENTITY:
                if (StringUtils.isNotBlank(context.getInputDuns())) {
                    parts.add(String.format("duns=%s", context.getInputDuns()));
                } else if (StringUtils.isNotBlank(context.getInputUrl())) {
                    parts.add(String.format("url=%s", context.getInputUrl()));
                    NameLocation nl = context.getInputNameLocation();
                    if (nl != null && StringUtils.isNotBlank(nl.getCountryCode())) {
                        parts.add(String.format("countryISOAlpha2Code=%s", nl.getCountryCode()));
                    }
                } else {
                    parts.addAll(getLocationParams(context.getInputNameLocation()));
                }
                break;
            case REALTIME_EMAIL:
                if (!StringUtils.isEmpty(context.getInputEmail())) {
                    parts.add(String.format("email=%s", urlEncode(context.getInputEmail())));
                    break;
                } else {
                    throw new LedpException(LedpCode.LEDP_25024);
                }
            default:
                throw new LedpException(LedpCode.LEDP_25025, new String[] { apiType.name() });
        }
        parts.add("candidateMaximumQuantity=1");
        parts.add("confidenceLowerLevelThresholdValue=1");
        parts.add("isCleanseAndStandardizeInformationRequired=true");
        if (context.getMatchRule() != null && //
                CollectionUtils.isNotEmpty(context.getMatchRule().getExclusionCriteria())) {
            String exclusions = StringUtils.join(context.getMatchRule().getExclusionCriteria().stream() //
                    .map(ExclusionCriterion::getUrlParam).collect(Collectors.toList()), ",");
            parts.add("exclusionCriteria=" + exclusions);
        }
        String params = StringUtils.join(parts, "&");
        if (log.isDebugEnabled()) {
            log.debug("params={}", params);
        }
        return params;
    }

    private static List<String> getLocationParams(NameLocation nl) {
        List<String> parts = new ArrayList<>();
        if (!isValidNameLocation(nl)) {
            throw new LedpException(LedpCode.LEDP_25023);
        }
        if (StringUtils.isNotBlank(nl.getName())) {
            parts.add(String.format("name=%s", urlEncode(nl.getName())));
        }
        if (StringUtils.isNotBlank(nl.getCountryCode())) {
            parts.add(String.format("countryISOAlpha2Code=%s", nl.getCountryCode()));
        }
        if (StringUtils.isNotBlank(nl.getRegistrationNumber())) {
            parts.add(String.format("registrationNumber=%s", urlEncode(nl.getRegistrationNumber())));
            if (StringUtils.isNotBlank(nl.getRegistrationNumberType())) {
                parts.add(String.format("registrationNumberType=%s", nl.getRegistrationNumberType()));
            }
        }
        if (StringUtils.isNotBlank(nl.getCity())) {
            parts.add(String.format("addressLocality=%s", urlEncode(nl.getCity())));
        }
        if (StringUtils.isNotBlank(nl.getState())) {
            String stateCode = LocationUtils.getStardardStateCode(nl.getCountry(), nl.getState());
            parts.add(String.format("addressRegion=%s", urlEncode(stateCode)));
        }
        if (StringUtils.isNotBlank(nl.getZipcode())) {
            parts.add(String.format("postalCode=%s", urlEncode(nl.getZipcode())));
        }
        if (StringUtils.isNotBlank(nl.getPhoneNumber())) {
            parts.add(String.format("telephoneNumber=%s", urlEncode(nl.getPhoneNumber())));
        }
        if (StringUtils.isNotBlank(nl.getStreet())) {
            parts.add(String.format("streetAddressLine1=%s", urlEncode(nl.getStreet())));
        }
        if (StringUtils.isNotBlank(nl.getStreet2())) {
            parts.add(String.format("streetAddressLine2=%s", urlEncode(nl.getStreet2())));
        }
        return parts;
    }

    public static Set<String> parseCacheableBlockIds(String response) {
        JsonNode root = JsonUtils.deserialize(response, JsonNode.class);
        JsonNode blockStatusList = JsonUtils.tryGetJsonNode(root,"blockStatus");
        Set<String> blockIds = new HashSet<>();
        if (blockStatusList != null) {
            for (JsonNode blockStatus: blockStatusList) {
                String status = JsonUtils.parseStringValueAtPath(blockStatus, "status");
                if ("ok".equalsIgnoreCase(status)) {
                    String blockId = JsonUtils.parseStringValueAtPath(blockStatus, "blockID");
                    if (!blockId.startsWith("baseinfo")) {
                        blockIds.add(blockId);
                    }
                }
            }
        }
        return blockIds;
    }

    public static String parseErrorCode(String response) {
        JsonNode root = JsonUtils.deserialize(response, JsonNode.class);
        return parseErrorCode(root);
    }

    private static String parseErrorCode(JsonNode root) {
        JsonNode errorNode = JsonUtils.tryGetJsonNode(root,"error");
        if (errorNode != null) {
            String errorCode = JsonUtils.parseStringValueAtPath(errorNode, "errorCode");
            if (StringUtils.isBlank(errorCode)) {
                errorCode = "00000"; // unknown error
            }
            return errorCode;
        }
        return null;
    }

    public static Map<String, Object> parseDataBlock(String response, List<PrimeColumn> metadata) {
        Map<String, Object> result = new HashMap<>();
        if (StringUtils.isNotBlank(response)) {
            JsonNode root = JsonUtils.deserialize(response, JsonNode.class);
            String errorCode = parseErrorCode(root);
            if (StringUtils.isBlank(errorCode)) {
                // cache of jsonPath -> jsonNode
                ConcurrentMap<String, JsonNode> nodeCache = new ConcurrentHashMap<>();
                metadata.forEach(md -> {
                    String jsonPath = md.getJsonPath();
                    JsonNode jsonNode = getNodeAt(root, jsonPath, nodeCache);
                    Object value = toTypedValue(jsonNode, md.getJavaClass());
                    String attrName = md.getAttrName();
                    result.put(attrName, value);
                });
            }
        }
        return result;
    }

    private static Object toTypedValue(JsonNode jsonNode, String javaClz) {
        if (jsonNode == null || JsonNodeType.NULL.equals(jsonNode.getNodeType()) || //
                JsonNodeType.MISSING.equals(jsonNode.getNodeType())) {
            return null;
        } else if (StringUtils.isBlank(javaClz) || "String".equals(javaClz)) {
            return jsonNode.asText();
        } else {
            Object result;
            switch (jsonNode.getNodeType()) {
                case BOOLEAN:
                    result = jsonNode.asBoolean();
                    break;
                case STRING:
                    result = jsonNode.asText();
                    break;
                case NUMBER:
                    switch (javaClz) {
                        case "Integer":
                            result = jsonNode.asInt();
                            break;
                        case "Long":
                            result = jsonNode.asLong();
                            break;
                        case "Float":
                        case "Double":
                        default:
                            result = jsonNode.asDouble();
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Cannot convert json node of type " //
                            + jsonNode.getNodeType() + " to a value object.");
            }
            return result;
        }
    }

    private static JsonNode getNodeAt(JsonNode root, String path, ConcurrentMap<String, JsonNode> nodeCache) {
        if (nodeCache.containsKey(path)) {
            return nodeCache.get(path);
        } else {
            if (path.contains(".")) {
                List<String> parts = Arrays.asList(path.split("\\."));
                String parent = StringUtils.join(parts.subList(0, parts.size() - 1), '.');

                JsonNode parentNode = nodeCache.getOrDefault(parent, null);
                if (parentNode == null) {
                    parentNode = getNodeAt(root, parent, nodeCache);
                }

                // directly inserting a null object into the map triggers a NullPointerException
                if (parentNode == null) {
                    return null;
                }
                nodeCache.putIfAbsent(parent, parentNode);

                if (parentNode instanceof ArrayNode) {
                    ArrayNode arrayNode = (ArrayNode) parentNode;
                    if (arrayNode.size() == 0) {
                        parentNode = null;
                    } else {
                        parentNode = parentNode.get(0);
                    }
                }
                String tail = parts.get(parts.size() - 1);
                if (parentNode != null && parentNode.has(tail)) {
                    return parentNode.get(tail);
                } else {
                    return null;
                }
            } else {
                return root.get(path);
            }
        }
    }

    public static void parseJsonResponse(String response, DnBMatchContext context, DnBAPIType apiType) {
        JsonNode jsonNode = JsonUtils.deserialize(response, JsonNode.class);
        if (DnBAPIType.REALTIME_ENTITY.equals(apiType)) {
            List<DnBMatchCandidate> candidates = parseRealTimeCandidates(jsonNode);
            if (context.getDuration() != null) {
                candidates.forEach(candidate -> candidate.setMatchDuration(context.getDuration()));
            }
            context.setCandidates(candidates);
        }
    }

    private static List<DnBMatchCandidate> parseRealTimeCandidates(JsonNode jsonNode) {
        List<DnBMatchCandidate> candidates = new ArrayList<>();
        String matchType = JsonUtils.parseStringValueAtPath(jsonNode, "matchDataCriteria");
        JsonNode candidatesNode = JsonUtils.tryGetJsonNode(jsonNode, "matchCandidates");
        if (candidatesNode != null) {
            for (JsonNode node: candidatesNode) {
                DnBMatchCandidate candidate = parseCandidate(node);
                candidate.setMatchType(matchType);
                candidates.add(candidate);
                if (candidates.size() >= 50) { // no more than 50 candidates
                    break;
                }
            }
        }
        return candidates;
    }

    private static DnBMatchCandidate parseCandidate(JsonNode jsonNode) {
        DnBMatchCandidate candidate = new DnBMatchCandidate();
        String duns = JsonUtils.parseStringValueAtPath(jsonNode, "organization", "duns");
        candidate.setDuns(duns);
        candidate.setOperatingStatus(JsonUtils.parseStringValueAtPath(jsonNode, "organization", "dunsControlStatus", "operatingStatus", "description"));
        JsonNode orgNode = JsonUtils.tryGetJsonNode(jsonNode, "organization");
        NameLocation nameLocation = parseNameLocation(orgNode);
        candidate.setNameLocation(nameLocation);
        candidate.setUnreachable(nameLocation.getUnreachable());

        JsonNode familyTreeNode = JsonUtils.tryGetJsonNode(jsonNode, "organization", "corporateLinkage", "familytreeRolesPlayed");
        if (familyTreeNode instanceof ArrayNode) {
            List<String> roles = new ArrayList<>();
            for (JsonNode node: familyTreeNode) {
                String role = node.get("description").asText();
                roles.add(role);
            }
            candidate.setFamilyTreeRoles(roles);
        }
        Boolean isMailUndeliverable = JsonUtils.parseBooleanValueAtPath(jsonNode, "organization", "dunsControlStatus", "isMailUndeliverable");
        candidate.setMailUndeliverable(isMailUndeliverable);

        // FIXME: (DCP-2144) not sure how to parse isMarketable
        // candidate.setMarketable(?);

        JsonNode insightNode = JsonUtils.tryGetJsonNode(jsonNode, "matchQualityInformation");
        if (insightNode != null) {
            DnBMatchInsight matchInsight = parseMatchInsight(insightNode);
            candidate.setMatchInsight(matchInsight);
        }
        return candidate;
    }

    private static NameLocation parseNameLocation(JsonNode orgNode) {
        NameLocation nameLocation = new NameLocation();
        String name = JsonUtils.parseStringValueAtPath(orgNode, "primaryName");
        nameLocation.setName(name);
        if (orgNode.has("telephone") && orgNode.get("telephone").size() > 0) {
            ArrayNode phoneNodes = (ArrayNode) orgNode.get("telephone");
            for (JsonNode phoneNode: phoneNodes) {
                String phoneNumber = JsonUtils.parseStringValueAtPath(phoneNode, "telephoneNumber");
                if (StringUtils.isNotBlank(phoneNumber)) {
                    nameLocation.setPhoneNumber(phoneNumber);
                    nameLocation.setUnreachable(JsonUtils.parseBooleanValueAtPath(phoneNode, "isUnreachable"));
                    break;
                }
            }
        }

        JsonNode addrNode = JsonUtils.tryGetJsonNode(orgNode, "primaryAddress");
        String city = JsonUtils.parseStringValueAtPath(addrNode, "addressLocality", "name");
        nameLocation.setCity(city);
        String state = JsonUtils.parseStringValueAtPath(addrNode, "addressRegion", "name");
        if (StringUtils.isBlank(state)) {
            state = JsonUtils.parseStringValueAtPath(addrNode, "addressRegion", "abbreviatedName");
        }
        nameLocation.setState(state);
        String country = JsonUtils.parseStringValueAtPath(addrNode, "addressCountry", "name");
        nameLocation.setCountry(country);
        String countryCode = JsonUtils.parseStringValueAtPath(addrNode, "addressCountry", "isoAlpha2Code");
        nameLocation.setCountryCode(countryCode);
        String zipCode = JsonUtils.parseStringValueAtPath(addrNode, "postalCode");
        nameLocation.setZipcode(zipCode);
        String zipCodeExt = JsonUtils.parseStringValueAtPath(addrNode, "postalCodeExtension");
        nameLocation.setZipcodeExtension(zipCodeExt);

        if (addrNode != null) {
            String addr = JsonUtils.parseStringValueAtPath(addrNode, "streetAddress", "line1");
            nameLocation.setStreet(addr);
            String addr2 = JsonUtils.parseStringValueAtPath(addrNode, "streetAddress", "line2");
            nameLocation.setStreet2(addr2);
        }
        return nameLocation;
    }

    private static DnBMatchInsight parseMatchInsight(JsonNode insightNode) {
        DnBMatchInsight matchInsight = new DnBMatchInsight();
        Double nameMatchScore = JsonUtils.parseDoubleValueAtPath(insightNode, "nameMatchScore");
        matchInsight.setNameMatchScore(nameMatchScore);
        Integer confidenceCode = JsonUtils.parseIntegerValueAtPath(insightNode, "confidenceCode");
        matchInsight.setConfidenceCode(confidenceCode);
        String matchGrade = JsonUtils.parseStringValueAtPath(insightNode, "matchGrade");
        matchInsight.setMatchGrade(new DnBMatchGrade(matchGrade));
        String dataProfile = JsonUtils.parseStringValueAtPath(insightNode, "matchDataProfile");
        matchInsight.setMatchDataProfile(new DnBMatchDataProfile(dataProfile));
        return matchInsight;
    }

    private static String urlEncode(String val) {
        try {
            return URLEncoder.encode(val, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isValidNameLocation(NameLocation nameLocation) {
        boolean hasName = StringUtils.isNotBlank(nameLocation.getName());
        boolean hasPhone = StringUtils.isNotBlank(nameLocation.getPhoneNumber());
        boolean hasRegNumber = StringUtils.isNotBlank(nameLocation.getRegistrationNumber());
        boolean hasCountryCode = StringUtils.isNotBlank(nameLocation.getCountryCode());
        return hasCountryCode && (hasName || hasPhone || hasRegNumber);
    }


}
