package com.latticeengines.datacloud.match.util;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchDataProfile;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchGrade;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchInsight;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public final class Direct2Utils {

    protected Direct2Utils() {
        throw new UnsupportedOperationException();
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
        JsonNode candidatesNode = JsonUtils.tryGetJsonNode(jsonNode, //
                "GetCleanseMatchResponse", //
                "GetCleanseMatchResponseDetail", //
                "MatchResponseDetail", //
                "MatchCandidate");
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
        candidate.setDuns(JsonUtils.parseStringValueAtPath(jsonNode, "DUNSNumber"));
        candidate.setOperatingStatus(JsonUtils.parseStringValueAtPath(jsonNode, "OperatingStatusText", "$"));
        NameLocation nameLocation = parseNameLocation(jsonNode);
        candidate.setNameLocation(nameLocation);

        JsonNode insightNode = JsonUtils.tryGetJsonNode(jsonNode, "MatchQualityInformation");
        if (insightNode != null) {
            DnBMatchInsight matchInsight = parseMatchInsight(insightNode);
            candidate.setMatchInsight(matchInsight);
        }
        return candidate;
    }

    private static NameLocation parseNameLocation(JsonNode json) {
        NameLocation nameLocation = new NameLocation();
        String name = JsonUtils.parseStringValueAtPath(json,
                "OrganizationPrimaryName", "OrganizationName", "$");
        nameLocation.setName(name);
        ArrayNode addrLines = (ArrayNode) JsonUtils.tryGetJsonNode(json, "PrimaryAddress", "StreetAddressLine");
        if (addrLines != null) {
            String addr = JsonUtils.parseStringValueAtPath(addrLines.get(0), "LineText");
            nameLocation.setStreet(addr);
            if (addrLines.size() > 1) {
                String addr2 = JsonUtils.parseStringValueAtPath(addrLines.get(1), "LineText");
                nameLocation.setStreet2(addr2);
            }
        }
        String city = JsonUtils.parseStringValueAtPath(json, "PrimaryAddress", "PrimaryTownName");
        nameLocation.setCity(city);
        String state = JsonUtils.parseStringValueAtPath(json, "PrimaryAddress", "TerritoryAbbreviatedName");
        nameLocation.setState(state);
        String countryCode = JsonUtils.parseStringValueAtPath(json, "PrimaryAddress", "CountryISOAlpha2Code");
        nameLocation.setCountryCode(countryCode);
        String zipCode = JsonUtils.parseStringValueAtPath(json, "PrimaryAddress", "PostalCode");
        nameLocation.setZipcode(zipCode);
        String zipCodeExt = JsonUtils.parseStringValueAtPath(json, "PrimaryAddress", "PostalCodeExtensionCode");
        nameLocation.setZipcodeExtension(zipCodeExt);

        String phoneNumber = JsonUtils.parseStringValueAtPath(json, "TelephoneNumber", "TelecommunicationNumber");
        nameLocation.setPhoneNumber(phoneNumber);
        return nameLocation;
    }

    private static DnBMatchInsight parseMatchInsight(JsonNode insightNode) {
        DnBMatchInsight matchInsight = new DnBMatchInsight();
        Integer confidenceCode = JsonUtils.parseIntegerValueAtPath(insightNode, "ConfidenceCodeValue");
        matchInsight.setConfidenceCode(confidenceCode);
        String matchGrade = JsonUtils.parseStringValueAtPath(insightNode, "MatchGradeText");
        matchInsight.setMatchGrade(new DnBMatchGrade(matchGrade));
        String dataProfile = JsonUtils.parseStringValueAtPath(insightNode, "MatchDataProfileText");
        matchInsight.setMatchDataProfile(new DnBMatchDataProfile(dataProfile));
        return matchInsight;
    }


}
