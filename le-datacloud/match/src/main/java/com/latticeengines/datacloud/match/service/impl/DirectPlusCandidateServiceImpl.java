package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.Classification;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.ConfidenceCode;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchDataProfile;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchGrade;
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

@Service
public class DirectPlusCandidateServiceImpl implements DirectPlusCandidateService {

    @Override
    public List<Object> parseCandidate(DnBMatchCandidate candidate) {
        List<Object> data = new ArrayList<>();
        data.add(candidate.getClassification().name()); // classification
        data.add(candidate.getMatchType()); // match type
        String duns = candidate.getDuns();
        data.add(duns); // duns
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
                MatchType, //
                MatchedDuns, //
                ConfidenceCode, //
                MatchGrade, //
                MatchDataProfile, //
                NameMatchScore, //
                OperatingStatusText //
        );
    }

    @Override
    public List<Pair<String, Class<?>>> candidateSchema() {
        return Arrays.asList( //
                Pair.of(Classification, String.class), //
                Pair.of(MatchType, String.class), //
                Pair.of(MatchedDuns, String.class), //
                Pair.of(ConfidenceCode, Integer.class), //
                Pair.of(MatchGrade, String.class), //
                Pair.of(MatchDataProfile, String.class), //
                Pair.of(NameMatchScore, String.class), //
                Pair.of(OperatingStatusText, String.class) //
        );
    }

    @Override
    public List<PrimeColumn> candidateColumns() {
        return Arrays.asList( //
                new PrimeColumn(MatchType, "Match Type", //
                        "matchDataCriteria"), //
                new PrimeColumn(MatchedDuns, "Matched D-U-N-S Number", //
                        "matchCandidates.organization.duns"), //
                new PrimeColumn(ConfidenceCode, "Match Confidence Code", //
                        "matchCandidates.matchQualityInformation.confidenceCode"), //
                new PrimeColumn(MatchGrade, "Match Grade", //
                        "matchCandidates.matchQualityInformation.matchGrade"), //
                new PrimeColumn(MatchDataProfile, "Match Data Profile", //
                        "matchCandidates.matchQualityInformation.matchDataProfile"), //
                new PrimeColumn(NameMatchScore, "Name Match Score", //
                        "matchCandidates.matchQualityInformation.nameMatchScore"), //
                new PrimeColumn(OperatingStatusText, "Match Candidate Operating Status", //
                        "matchCandidates.organization.dunsControlStatus.operatingStatus.description") //
        );
    }

}
