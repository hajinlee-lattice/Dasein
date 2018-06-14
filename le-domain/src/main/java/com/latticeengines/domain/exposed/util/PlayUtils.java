package com.latticeengines.domain.exposed.util;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;

public class PlayUtils {

    public static void validatePlayBeforeLaunch(String playName, Play play) {
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_18149, new String[] { playName });
        }

        if (play.getRatingEngine() == null) {
            throw new LedpException(LedpCode.LEDP_18149, new String[] { play.getName() });
        } else if (play.getRatingEngine().getStatus() != RatingEngineStatus.ACTIVE) {
            throw new LedpException(LedpCode.LEDP_18155, new String[] { play.getName() });
        }

    }

    public static void validatePlayLaunchBeforeLaunch(String customerSpace, PlayLaunch playLaunch, Play play) {
        if (CollectionUtils.isEmpty(playLaunch.getBucketsToLaunch())) {
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
    }

}
