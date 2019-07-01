package com.latticeengines.domain.exposed.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;

public class PlayUtils {

    public static void validatePlay(Play play) {
        if (play.getRatingEngine() != null && play.getRatingEngine().getStatus() != RatingEngineStatus.ACTIVE) {
            throw new LedpException(LedpCode.LEDP_18155, new String[] { play.getName() });
        }
    }

    public static void validatePlayLaunchBeforeLaunch(PlayLaunch playLaunch, Play play) {
        if (play.getRatingEngine() == null) {
            playLaunch.setBucketsToLaunch(Collections.emptySet());
            if (!playLaunch.isLaunchUnscored()) {
                throw new LedpException(LedpCode.LEDP_18212, new String[] { play.getName() });
            }
        }
        if (play.getRatingEngine() != null && !playLaunch.isLaunchUnscored()
                && CollectionUtils.isEmpty(playLaunch.getBucketsToLaunch())) {
            // TODO - enable it once UI has fix for PLS-6769
            // throw new LedpException(LedpCode.LEDP_18156, new String[] {
            // play.getName() });

            // ----------------

            // TODO - remove it when (PLS-6769) if done
            // if no buckets are specified then we default it to all buckets
            Set<RatingBucketName> defaultBucketsToLaunch = //
                    new TreeSet<>(Arrays.asList(RatingBucketName.values()));
            playLaunch.setBucketsToLaunch(defaultBucketsToLaunch);
        }

        if (StringUtils.isBlank(playLaunch.getDestinationOrgId()) || playLaunch.getDestinationSysType() == null) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "No destination system selected for the launch for play: " + play.getName() });
        }

        if (playLaunch.getExcludeItemsWithoutSalesforceId()
                && StringUtils.isBlank(playLaunch.getDestinationAccountId())) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {
                    "Cannot restrict accounts with null Ids if account id has not been set up for selected destination" });
        }
    }

}
