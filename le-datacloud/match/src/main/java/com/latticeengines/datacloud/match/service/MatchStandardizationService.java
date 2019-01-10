package com.latticeengines.datacloud.match.service;

import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MatchStandardizationService {

    void parseRecordForDomain(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                              Set<String> domainSet, boolean treatPublicDomainAsNormal, EntityMatchKeyRecord record);

    void parseRecordForNameLocation(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                    Set<NameLocation> nameLocationSet, EntityMatchKeyRecord record);

    void parseRecordForDuns(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                    EntityMatchKeyRecord record);

    void parseRecordForSystemIds(List<Object> inputRecord, Map<MatchKey, List<String>> keyMap,
                                 Map<MatchKey, List<Integer>> keyPositionMap, MatchKeyTuple matchKeyTuple);

    // TODO(jwinter): The two methods below are not used right now but I'm not deleting them in case they are needed
    //     later.
    /*
    void parseRecordForLatticeAccountId(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                        EntityMatchKeyRecord record);

    void parseRecordForLookupId(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                EntityMatchKeyRecord record);
    */
}
