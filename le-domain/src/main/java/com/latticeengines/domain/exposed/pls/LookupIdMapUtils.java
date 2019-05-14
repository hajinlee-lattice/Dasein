package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;

public class LookupIdMapUtils {

    public static Pair<String, String> getEffectiveOrgInfo(Map<String, String> orgInfo) {
        Pair<String, String> effectiveOrgInfo = null;

        if (MapUtils.isNotEmpty(orgInfo)) {
            if (StringUtils.isNotBlank(orgInfo.get(CDLConstants.ORG_ID))
                    && StringUtils.isNotBlank(orgInfo.get(CDLConstants.EXTERNAL_SYSTEM_TYPE))) {
                effectiveOrgInfo = new ImmutablePair<>(orgInfo.get(CDLConstants.ORG_ID).trim(),
                        orgInfo.get(CDLConstants.EXTERNAL_SYSTEM_TYPE).trim());
            }
        }
        return effectiveOrgInfo;
    }

    public static Map<String, List<LookupIdMap>> listToMap(List<LookupIdMap> connections) {
        Map<String, List<LookupIdMap>> toReturn = new HashMap<>();

        if (CollectionUtils.isNotEmpty(connections)) {
            connections //
                    .forEach(c -> {
                        CDLExternalSystemType type = c.getExternalSystemType();
                        List<LookupIdMap> listForType = toReturn.computeIfAbsent(type.name(), k -> new ArrayList<>());
                        listForType.add(c);
                    });
        }
        return toReturn;
    }

}
