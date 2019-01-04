package com.latticeengines.datacloud.match.service;

import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MatchStandardizationService {

    void parseRecordForDomain(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                              Set<String> domainSet, boolean treatPublicDomainAsNormal, InternalOutputRecord record);

    void parseRecordForNameLocation(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                    Set<NameLocation> nameLocationSet, InternalOutputRecord record);

    void parseRecordForDuns(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                    InternalOutputRecord record);

    void parseRecordForLatticeAccountId(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                        InternalOutputRecord record);

    void parseRecordForLookupId(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                InternalOutputRecord record);

    void parseRecordForSystemIds(List<Object> inputRecord, Map<MatchKey, List<String>> keyMap,
                                 Map<MatchKey, List<Integer>> keyPositionMap, MatchKeyTuple matchKeyTuple);
}
