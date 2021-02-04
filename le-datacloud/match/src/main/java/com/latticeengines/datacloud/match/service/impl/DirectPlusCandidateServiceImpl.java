package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.Classification;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.ConfidenceCode;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchDataProfile;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchGrade;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchIso2CountryCode;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchPrimaryBusinessName;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchType;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchedDuns;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.NameMatchScore;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.OperatingStatusText;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import com.latticeengines.datacloud.match.service.DirectPlusCandidateService;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchInsight;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

@Service
public class DirectPlusCandidateServiceImpl implements DirectPlusCandidateService {

    @Override
    public List<Object> parseCandidate(DnBMatchCandidate candidate) {
        List<Object> data = new ArrayList<>();
        data.add(candidate.getClassification().name()); // classification
        String duns = candidate.getDuns();
        data.add(duns); // duns
        data.add(candidate.getMatchType()); // match type
        if (candidate.getMatchInsight() != null) {
            DnBMatchInsight matchInsight = candidate.getMatchInsight();
            data.add(matchInsight.getConfidenceCode());
            data.add(matchInsight.getMatchGrade().getRawCode());
            data.add(matchInsight.getMatchDataProfile().getText());
            data.add(matchInsight.getNameMatchScore());
        } else {
            data.add(null); // confidence code
            data.add(null); // match grade
            data.add(null); // match data profile
            data.add(null); // name match score
        }
        data.add(candidate.getOperatingStatus());
        if (candidate.getNameLocation() != null) {
            NameLocation location = candidate.getNameLocation();
            data.add(location.getName());
            data.add(location.getCountryCode());
        } else {
            data.add(null); // primary name
            data.add(null); // country code
        }
        return data;
    }

    @Override
    public List<Object> emptyCandidate() {
        List<Object> data = new ArrayList<>();
        for (int i = 0; i < candidateOutputFields().size(); i++) {
            data.add(null);
        }
        return data;
    }

    @Override
    public List<String> candidateOutputFields() {
        // hard coded for now, need to in sync with SplitImportMatchResult
        return Arrays.asList( //
                Classification, //
                MatchedDuns, //
                MatchType, //
                ConfidenceCode, //
                MatchGrade, //
                MatchDataProfile, //
                NameMatchScore, //
                OperatingStatusText, //
                MatchPrimaryBusinessName, //
                MatchIso2CountryCode //
        );
    }

    @Override
    public List<Pair<String, Class<?>>> candidateSchema() {
        return Arrays.asList( //
                Pair.of(Classification, String.class), //
                Pair.of(MatchedDuns, String.class), //
                Pair.of(MatchType, String.class), //
                Pair.of(ConfidenceCode, Integer.class), //
                Pair.of(MatchGrade, String.class), //
                Pair.of(MatchDataProfile, String.class), //
                Pair.of(NameMatchScore, String.class), //
                Pair.of(OperatingStatusText, String.class), //
                Pair.of(MatchPrimaryBusinessName, String.class), //
                Pair.of(MatchIso2CountryCode, String.class) //
        );
    }

    @Override
    public List<PrimeColumn> candidateColumns() {
        return Arrays.asList( //
                new PrimeColumn(MatchedDuns, "Matched D-U-N-S Number", //
                        "matchCandidates.organization.duns", //
                        "String", //
                        "The D-U-N-S Number, assigned by Dun & Bradstreet, is an identification number that uniquely identifies the entity in accordance with the Data Universal Numbering System (D-U-N-S).", //
                        "123456789"), //
                new PrimeColumn(MatchType, "Match Type", //
                        "matchDataCriteria", //
                        "String", //
                        "The type of match performed to identify the identified entity.  May be one of the following: " + //
                                "- DUNS Number Lookup: D-U-N-S Number was used to perform match. " + //
                                "- Name and Address Lookup: Name and Address were used to perform match. " + //
                                "- National ID Lookup: Registration Number was used to perform match. " + //
                                "- Telephone Number Lookup: Telephone Number was used to perform match. " + //
                                "- ZIP Code Lookup: Postal Code was used to perform match.", //
                        "Name and Address Lookup"), //
                new PrimeColumn(ConfidenceCode, "Match Confidence Code", //
                        "matchCandidates.matchQualityInformation.confidenceCode", //
                        "Integer", //
                        "A numeric value from 1 (low) up to 10 (high) indicating the level of certainty at which this possible candidate was included in this result set.", //
                        "6"), //
                new PrimeColumn(MatchGrade, "Match Grade", //
                        "matchCandidates.matchQualityInformation.matchGrade", //
                        "String", //
                        "The compound rating that comprises one rating per type of data scored by a match engine. " +
                                "For example, in AABZBAAAFBZ the &quot;A&quot; in the 1st position means that the Name scored between 80-100 and the &quot;A&quot; in the 2nd position means that the Street Number had to score exactly 100. " +
                                "For more details, please see the Identity Resolution guide.", //
                        "AZZZZZZZFZZ"), //
                new PrimeColumn(MatchDataProfile, "Match Data Profile", //
                        "matchCandidates.matchQualityInformation.matchDataProfile",
                        "String",
                        "The information that the matching process used to arrive at a match in addition to the application's business rules and parameters specified by the requestor. " +
                                "This is a 28-byte string consisting of 14 components of 2 bytes each. " +
                                "The 14 components are, in order, Name, Street Number, Street Name, Town Name, Territory Name, P.O. Box, Telephone, Post Code, D-U-N-S Number, Industry Code, Geographical Density, Location Uniqueness, Registration Number and URL.", //
                        "0000000000989800000000009898"), //
                new PrimeColumn(NameMatchScore, "Name Match Score", //
                        "matchCandidates.matchQualityInformation.nameMatchScore",
                        "Double",
                        "A number from -1 to 100 (including decimals) that indicates the detailed level of match on the name of the matched candidate. " +  //
                                "- 0 indicates no match." + //
                                "- 100 indicates most accurate match.",  //
                        "85.5"), //
                new PrimeColumn(OperatingStatusText, "Match Candidate Operating Status", //
                        "matchCandidates.organization.dunsControlStatus.operatingStatus.description", //
                        "String", //
                        "The entity's functional state or trading status summarized into either active or out of business.", //
                        "Active"), //
                new PrimeColumn(MatchPrimaryBusinessName, "Match Primary Business Name", //
                        "matchCandidates.organization.primaryName",  //
                        "String",  //
                        "The single name by which the entity is primarily known or identified.",  //
                        "Bayer AG"), //
                new PrimeColumn(MatchIso2CountryCode, "Match ISO Alpha 2 Char Country Code", //
                        "matchCandidates.organization.primaryAddress.addressCountry.isoAlpha2Code", //
                        "String", //
                        "The two-letter country code, defined by the International Organization for Standardization (ISO) ISO 3166-1 scheme identifying the country/market in which this address is located.", //
                        "DE") //
        );
    }

}
