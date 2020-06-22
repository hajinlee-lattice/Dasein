package com.latticeengines.datacloud.match.service;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;

public interface DirectPlusCandidateService {

    List<Object> parseCandidate(DnBMatchCandidate candidate);

    List<Object> emptyCandidate();

    List<String> candidateOutputFields();

    List<Pair<String, Class<?>>> candidateSchema();

}
