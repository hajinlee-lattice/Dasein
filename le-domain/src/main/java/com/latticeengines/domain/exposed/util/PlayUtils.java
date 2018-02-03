package com.latticeengines.domain.exposed.util;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;

public class PlayUtils {

    public static void validatePlayBeforeLaunch(Play play) {
        if (play.getRatingEngine() == null) {
            throw new LedpException(LedpCode.LEDP_18149, new String[] { play.getName() });
        } else if (play.getRatingEngine().getStatus() != RatingEngineStatus.ACTIVE) {
            throw new LedpException(LedpCode.LEDP_18155, new String[] { play.getName() });
        }

    }

    public static void validatePlayLaunchBeforeLaunch(String customerSpace, PlayLaunch playLaunch, Play play) {
        if (CollectionUtils.isEmpty(playLaunch.getBucketsToLaunch())) {
            throw new LedpException(LedpCode.LEDP_18156, new String[] { play.getName() });
        }
    }

}
