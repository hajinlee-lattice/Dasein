package com.latticeengines.datacloud.match.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchDataProfile;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchGrade;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchInsight;
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
                if (!StringUtils.isEmpty(context.getInputNameLocation().getName())) {
                    parts.add(String.format("name=%s", urlEncode(context.getInputNameLocation().getName())));
                } else {
                    throw new LedpException(LedpCode.LEDP_25023);
                }
                if (!StringUtils.isEmpty(context.getInputNameLocation().getCountryCode())) {
                    parts.add(String.format("countryISOAlpha2Code=%s", context.getInputNameLocation().getCountryCode()));
                } else {
                    throw new LedpException(LedpCode.LEDP_25023);
                }
                if (!StringUtils.isEmpty(context.getInputNameLocation().getCity())) {
                    parts.add(String.format("addressLocality=%s", urlEncode(context.getInputNameLocation().getCity())));
                }
                if (!StringUtils.isEmpty(context.getInputNameLocation().getState())) {
                    String stateCode = LocationUtils.getStardardStateCode(context.getInputNameLocation().getCountry(),
                            context.getInputNameLocation().getState());
                    parts.add(String.format("addressRegion=%s", urlEncode(stateCode)));
                }
                if (StringUtils.isNotEmpty(context.getInputNameLocation().getZipcode())) {
                    parts.add(String.format("postalCode=%s", urlEncode(context.getInputNameLocation().getZipcode())));
                }
                if (StringUtils.isNotEmpty(context.getInputNameLocation().getPhoneNumber())) {
                    parts.add(String.format("telephoneNumber=%s", urlEncode(context.getInputNameLocation().getPhoneNumber())));
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
        return StringUtils.join(parts, "&");
    }

    public static void parseJsonResponse(String response, DnBMatchContext context, DnBAPIType apiType) {
        JsonNode jsonNode = JsonUtils.deserialize(response, JsonNode.class);
        if (DnBAPIType.REALTIME_ENTITY.equals(apiType)) {
            List<DnBMatchCandidate> candidates = parseRealTimeCandidates(jsonNode);
            context.setCandidates(candidates);
        }
    }

    private static List<DnBMatchCandidate> parseRealTimeCandidates(JsonNode jsonNode) {
        List<DnBMatchCandidate> candidates = new ArrayList<>();
        JsonNode candidatesNode = JsonUtils.tryGetJsonNode(jsonNode, "matchCandidates");
        if (candidatesNode != null) {
            for (JsonNode node: candidatesNode) {
                candidates.add(parseCandidate(node));
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
        NameLocation nameLocation = parseNameLocation(orgNode, duns);
        candidate.setNameLocation(nameLocation);

        JsonNode insightNode = JsonUtils.tryGetJsonNode(jsonNode, "matchQualityInformation");
        if (insightNode != null) {
            DnBMatchInsight matchInsight = parseMatchInsight(insightNode);
            candidate.setMatchInsight(matchInsight);
        }
        return candidate;
    }

    private static NameLocation parseNameLocation(JsonNode orgNode, String duns) {
        NameLocation nameLocation = new NameLocation();
        String name = JsonUtils.parseStringValueAtPath(orgNode, "primaryName");
        nameLocation.setName(name);
        if (orgNode.has("telephone") && orgNode.get("telephone").size() > 0) {
            ArrayNode phoneNodes = (ArrayNode) orgNode.get("telephone");
            for (JsonNode phoneNode: phoneNodes) {
                String phoneNumber = JsonUtils.parseStringValueAtPath(phoneNode, "telephoneNumber");
                if (StringUtils.isNotBlank(phoneNumber)) {
                    nameLocation.setPhoneNumber(phoneNumber);
                    break;
                }
            }
        }

//        // trying to get an example of tradeStyleNames output
//        if (orgNode.has("tradeStyleNames") && orgNode.get("tradeStyleNames").size() > 0) {
//            ArrayNode nameNodes = (ArrayNode) orgNode.get("telephone");
//            for (JsonNode nameNode: nameNodes) {
//                String tradeStyleName = JsonUtils.parseStringValueAtPath(nameNode, "name");
//                if (StringUtils.isNotBlank(tradeStyleName)) {
//                    nameLocation.setTradeStyleName(tradeStyleName);
//                    break;
//                }
//            }
//        }

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


}
