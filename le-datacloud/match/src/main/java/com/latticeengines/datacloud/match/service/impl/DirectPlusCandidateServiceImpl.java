package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import com.latticeengines.datacloud.match.service.DirectPlusCandidateService;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchInsight;

@Service
public class DirectPlusCandidateServiceImpl implements DirectPlusCandidateService {

    @Override
    public List<Object> parseCandidate(DnBMatchCandidate candidate) {
        List<Object> data = new ArrayList<>();
        String duns = candidate.getDuns();
        data.add(duns);
//        if (candidate.getNameLocation() != null) {
//            NameLocation nl = candidate.getNameLocation();
//            data.add(nl.getName());
//            data.add(nl.getStreet());
//            data.add(nl.getStreet2());
//            data.add(nl.getCity());
//            data.add(nl.getState());
//            data.add(nl.getZipcode());
//            data.add(nl.getCountryCode());
//            data.add(nl.getPhoneNumber());
//        } else {
//            data.add(null); // name
//            data.add(null); // street
//            data.add(null); // street 2
//            data.add(null); // city
//            data.add(null); // state
//            data.add(null); // zip code
//            data.add(null); // country code
//            data.add(null); // phone
//        }
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
        return Arrays.asList(
                "DunsNumber",
                "ConfidenceCode",
                "MatchGrade",
                "MatchDataProfile",
                "NameMatchScore",
                "OperatingStatusText"
        );
    }

    @Override
    public List<Pair<String, Class<?>>> candidateSchema() {
        return Arrays.asList( //
                Pair.of("DunsNumber", String.class), //
                Pair.of("ConfidenceCode", Integer.class), //
                Pair.of("MatchGrade", String.class), //
                Pair.of("MatchDataProfile", String.class), //
                Pair.of("NameMatchScore", String.class), //
                Pair.of("OperatingStatusText", String.class) //
        );
    }

}
