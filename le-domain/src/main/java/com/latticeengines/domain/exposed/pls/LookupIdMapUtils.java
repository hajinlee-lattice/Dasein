package com.latticeengines.domain.exposed.pls;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.cdl.CDLConstants;

public class LookupIdMapUtils {

    public static Pair<String, String> getEffectiveOrgInfo(Map<String, String> orgInfo) {
        Pair<String, String> effectiveOrgInfo = null;

        if (MapUtils.isNotEmpty(orgInfo)) {
            if (StringUtils.isNotBlank(orgInfo.get(CDLConstants.ORG_ID))
                    && StringUtils.isNotBlank(orgInfo.get(CDLConstants.EXTERNAL_SYSTEM_TYPE))) {
                effectiveOrgInfo = new ImmutablePair<String, String>(
                        orgInfo.get(CDLConstants.ORG_ID).trim(),
                        orgInfo.get(CDLConstants.EXTERNAL_SYSTEM_TYPE).trim());
            }
        }
        return effectiveOrgInfo;
    }

}
