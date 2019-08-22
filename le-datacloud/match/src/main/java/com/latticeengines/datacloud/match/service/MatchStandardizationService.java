package com.latticeengines.datacloud.match.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public interface MatchStandardizationService {

    boolean hasMultiDomain(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap);

    void parseRecordForDomain(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
            boolean treatPublicDomainAsNormal, EntityMatchKeyRecord record);

    void parseRecordForNameLocation(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
            Set<NameLocation> nameLocationSet, EntityMatchKeyRecord record);

    void parseRecordForDuns(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
            EntityMatchKeyRecord record);

    void parseRecordForSystemIds(List<Object> inputRecord, Map<MatchKey, List<String>> keyMap,
            Map<MatchKey, List<Integer>> keyPositionMap, MatchKeyTuple matchKeyTuple, EntityMatchKeyRecord record);

    /**
     * Find the first valid preferred entity ID and standardize it. Set both
     * original and standardized values to {@link EntityMatchKeyRecord}.
     *
     * @param entity
     *            target entity
     * @param inputRecord
     *            input rows
     * @param keyPositionMap
     *            position of columns for each match key
     * @param record
     *            target record to set the ID values
     */
    void parseRecordForPreferredEntityId(@NotNull String entity, @NotNull List<Object> inputRecord,
            @NotNull Map<MatchKey, List<Integer>> keyPositionMap, @NotNull EntityMatchKeyRecord record);

    void parseRecordForContact(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
            EntityMatchKeyRecord record);

    // TODO(jwinter): The two methods below are not used right now but I'm not
    // deleting them in case they are needed
    // later.
    /*
     * void parseRecordForLatticeAccountId(List<Object> inputRecord, Map<MatchKey,
     * List<Integer>> keyPositionMap, EntityMatchKeyRecord record);
     *
     * void parseRecordForLookupId(List<Object> inputRecord, Map<MatchKey,
     * List<Integer>> keyPositionMap, EntityMatchKeyRecord record);
     */
}
