package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayGroup;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("playProxy")
public class PlayProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final Logger log = LoggerFactory.getLogger(PlayProxy.class);

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/plays";

    private static final String DASHBOARD_URL = URL_PREFIX + "/launches/dashboard";

    private static final String DASHBOARD_COUNT_URL = DASHBOARD_URL + "/count";

    private static final String PLAY_TYPE_URL_PREFIX = "/customerspaces/{customerSpace}/playtypes";

    private static final String PLAY_GROUP_URL_PREFIX = "/customerspaces/{customerSpace}/playgroups";

    public PlayProxy() {
        super("cdl");
    }

    public List<Play> getPlays(String customerSpace, Boolean shouldLoadCoverage, String ratingEngineId) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();

        if (shouldLoadCoverage != null) {
            params.add("should-load-coverage=" + shouldLoadCoverage);
        }
        if (StringUtils.isNotBlank(ratingEngineId)) {
            params.add("rating-engine-id=" + ratingEngineId);
        }

        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }

        log.info("url is " + url);
        List<?> list = get("get plays", url, List.class);
        return JsonUtils.convertList(list, Play.class);
    }

    public List<String> getDeletedPlayIds(String customerSpace, Boolean forCleanupOnly) {
        String url = constructUrl(URL_PREFIX + "/deleted-play-ids", shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (forCleanupOnly == Boolean.TRUE) {
            params.add("for-cleanup-only=" + true);
        }

        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }

        if (!forCleanupOnly) {
            // avoid printing this log statement for quartz based cleanup
            log.info("url is " + url);
        }
        List<?> list = get("get deleted play ids", url, List.class);
        return JsonUtils.convertList(list, String.class);
    }

    public PlayLaunchDashboard getPlayLaunchDashboard(String customerSpace, String playName,
            List<LaunchState> launchStates, Long startTimestamp, Long offset, Long max, String sortby,
            Boolean descending, Long endTimestamp, String orgId, String externalSysType) {

        String url = constructUrl(DASHBOARD_URL, shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (StringUtils.isNotBlank(playName)) {
            params.add("play-name=" + playName);
        }
        if (CollectionUtils.isNotEmpty(launchStates)) {
            for (LaunchState state : launchStates) {
                params.add("launch-state=" + state);
            }
        }
        if (startTimestamp != null) {
            params.add("start-timestamp=" + startTimestamp);
        }
        if (offset != null) {
            params.add("offset=" + offset);
        }
        if (max != null) {
            params.add("max=" + max);
        }
        if (StringUtils.isNotBlank(sortby)) {
            params.add("sortby=" + sortby);
        }
        if (descending != null) {
            params.add("descending=" + descending);
        }
        if (endTimestamp != null) {
            params.add("end-timestamp=" + endTimestamp);
        }
        if (StringUtils.isNotBlank(orgId)) { // &&
                                             // StringUtils.isNotBlank(externalSysType)
            params.add("org-id=" + orgId.trim());
            params.add("external-sys-type=" + externalSysType.trim());
        }
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        log.info("url is " + url);
        return get("get PlayLaunchDashboard", url, PlayLaunchDashboard.class);
    }

    public Long getPlayLaunchDashboardEntriesCount(String customerSpace, String playName,
            List<LaunchState> launchStates, Long startTimestamp, Long endTimestamp, String orgId,
            String externalSysType) {
        String url = constructUrl(DASHBOARD_COUNT_URL, shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (StringUtils.isNotBlank(playName)) {
            params.add("play-name=" + playName);
        }
        if (CollectionUtils.isNotEmpty(launchStates)) {
            for (LaunchState state : launchStates) {
                params.add("launch-state=" + state);
            }
        }
        if (startTimestamp != null) {
            params.add("start-timestamp=" + startTimestamp);
        }
        if (endTimestamp != null) {
            params.add("end-timestamp=" + endTimestamp);
        }
        if (StringUtils.isNotBlank(orgId) && StringUtils.isNotBlank(externalSysType)) {
            params.add("org-id=" + orgId.trim());
            params.add("external-sys-type=" + externalSysType.trim());
        }
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        log.info("url is " + url);
        return get("get PlayLaunch Dashboard Entries Count", url, Long.class);
    }

    public Play getPlay(String customerSpace, String playName) {
        return getPlay(customerSpace, playName, false, true);
    }

    public Play getPlay(String customerSpace, String playName, boolean considerDeleted, boolean shouldLoadCoverage) {
        String url = constructUrl(URL_PREFIX + "/{playName}", shortenCustomerSpace(customerSpace), playName);
        url += "?consider-deleted=" + considerDeleted;
        url += "&should-load-coverage=" + shouldLoadCoverage;

        log.info("url is " + url);
        return get("get Play", url, Play.class);
    }

    public Play createOrUpdatePlay(String customerSpace, Play play) {
        return createOrUpdatePlay(customerSpace, play, true);
    }

    public Play createOrUpdatePlay(String customerSpace, Play play, boolean shouldLoadCoverage) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        url += "?should-load-coverage=" + shouldLoadCoverage;
        log.info("url is " + url);
        return post("create or update play", url, play, Play.class);
    }

    public void publishTalkingPoints(String customerSpace, String playName) {
        String url = constructUrl(URL_PREFIX + "/{playName}/talkingpoints/publish", shortenCustomerSpace(customerSpace),
                playName);
        log.info("url is " + url);
        post(String.format("publish talking points for play %s", playName), url, null, Void.class);
    }

    public void deletePlay(String customerSpace, String playName, boolean hardDelete) {
        String url = constructUrl(URL_PREFIX + "/{playName}", shortenCustomerSpace(customerSpace), playName);
        log.info("url is " + url);
        List<String> params = new ArrayList<>();
        params.add("hard-delete=" + hardDelete);
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        delete("Delete a play", url);
    }

    public PlayLaunch createPlayLaunch(String customerSpace, String playName, PlayLaunch playLaunch) {
        String url = constructUrl(URL_PREFIX + "/{playName}/launches", shortenCustomerSpace(customerSpace), playName);
        log.info("url is " + url);
        return post("create play launch", url, playLaunch, PlayLaunch.class);
    }

    public PlayLaunch updatePlayLaunch(String customerSpace, String playName, String launchId, PlayLaunch pLayLaunch) {
        String url = constructUrl(URL_PREFIX + "/{playName}/launches/{launchId}", shortenCustomerSpace(customerSpace),
                playName, launchId);
        log.info("url is " + url);
        return post("update play launch", url, pLayLaunch, PlayLaunch.class);
    }

    public PlayLaunch launchPlay(String customerSpace, String playName, String launchId, boolean isDryRunMode) {
        String url = constructUrl(URL_PREFIX + "/{playName}/launches/{launchId}/launch?dry-run={isDryRunMode}",
                shortenCustomerSpace(customerSpace), playName, launchId, isDryRunMode);
        log.info("url is " + url);
        return post("Launchplay", url, null, PlayLaunch.class);
    }

    @SuppressWarnings("rawtypes")
    public List<PlayLaunch> getPlayLaunches(String customerSpace, String playName, List<LaunchState> launchStates) {
        String url = constructUrl(URL_PREFIX + "/{playName}/launches", shortenCustomerSpace(customerSpace), playName);
        List<String> params = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(launchStates)) {
            for (LaunchState state : launchStates) {
                params.add("launch-state=" + state);
            }
        }
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        log.info("url is " + url);
        List list = get("get Play Launches", url, List.class);
        return JsonUtils.convertList(list, PlayLaunch.class);
    }

    public PlayLaunch getPlayLaunch(String customerSpace, String playName, String launchId) {
        String url = constructUrl(URL_PREFIX + "/{playName}/launches/{launchId}", shortenCustomerSpace(customerSpace),
                playName, launchId);
        log.info("url is " + url);
        return get("get Play Launch", url, PlayLaunch.class);
    }

    public PlayLaunch updatePlayLaunchProgress(String customerSpace, String playName, String launchId,
            Double launchCompletionPercent, Long accountsLaunched, Long contactsLaunched, Long accountsErrored,
            Long accountsSuppressed) {
        String url = constructUrl(URL_PREFIX + "/{playName}/launches/{launchId}", shortenCustomerSpace(customerSpace),
                playName, launchId);
        List<String> params = new ArrayList<>();
        if (launchCompletionPercent != null) {
            params.add("launchCompletionPercent=" + launchCompletionPercent);
        }
        if (accountsLaunched != null) {
            params.add("accountsLaunched=" + accountsLaunched);
        }
        if (contactsLaunched != null) {
            params.add("contactsLaunched=" + contactsLaunched);
        }
        if (accountsErrored != null) {
            params.add("accountsErrored=" + accountsErrored);
        }
        if (accountsSuppressed != null) {
            params.add("accountsSuppressed=" + accountsSuppressed);
        }
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        log.info("url is " + url);

        return patch("update PlayLaunch Progress ", url, null, PlayLaunch.class);
    }

    public void updatePlayLaunch(String customerSpace, String playName, String launchId, LaunchState action) {
        String url = constructUrl(URL_PREFIX + "/{playName}/launches/{launchId}/{action}",
                shortenCustomerSpace(customerSpace), playName, launchId, action);
        log.info("url is " + url);
        put("update PlayLaunch ", url);
    }

    public void deletePlayLaunch(String customerSpace, String playName, String launchId, boolean hardDelete) {
        String url = constructUrl(URL_PREFIX + "/{playName}/launches/{launchId}", shortenCustomerSpace(customerSpace),
                playName, launchId);
        List<String> params = new ArrayList<>();
        params.add("hard-delete=" + hardDelete);
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }

        log.info("url is " + url);
        delete("delete PlayLaunch ", url);
    }

    public List<PlayType> getPlayTypes(String customerSpace) {
        String url = constructUrl(PLAY_TYPE_URL_PREFIX, shortenCustomerSpace(customerSpace));
        log.info("url is " + url);
        return getList("get all Play Types", url, PlayType.class);
    }

    public PlayType createPlayType(String customerSpace, PlayType playType) {
        String url = constructUrl(PLAY_TYPE_URL_PREFIX, shortenCustomerSpace(customerSpace));
        log.info("url is " + url);
        return post("create new Play Types", url, playType, PlayType.class);
    }

    public PlayType getPlayTypeById(String customerSpace, String playTypeId) {
        String url = constructUrl(PLAY_TYPE_URL_PREFIX + "/{playTypeId}", shortenCustomerSpace(customerSpace),
                playTypeId);
        log.info("url is " + url);
        return get("get Play Type", url, PlayType.class);
    }

    public void updatePlayType(String customerSpace, String playTypeId, PlayType playType) {
        String url = constructUrl(PLAY_TYPE_URL_PREFIX + "/{playTypeId}", shortenCustomerSpace(customerSpace),
                playTypeId);
        log.info("url is " + url);
        post("create new Play Types", url, playType, PlayType.class);
    }

    public void deletePlayTypeById(String customerSpace, String playTypeId) {
        String url = constructUrl(PLAY_TYPE_URL_PREFIX + "/{playTypeId}", shortenCustomerSpace(customerSpace),
                playTypeId);
        log.info("url is " + url);
        delete("delete Play Type", url);
    }

    public List<PlayGroup> getPlayGroups(String customerSpace) {
        String url = constructUrl(PLAY_GROUP_URL_PREFIX, shortenCustomerSpace(customerSpace));
        log.info("url is " + url);
        return getList("get all Play Groups", url, PlayGroup.class);
    }

    public PlayGroup createPlayGroup(String customerSpace, PlayGroup playGroup) {
        String url = constructUrl(PLAY_GROUP_URL_PREFIX, shortenCustomerSpace(customerSpace));
        log.info("url is " + url);
        return post("create new Play Groups", url, playGroup, PlayGroup.class);
    }

    public PlayGroup getPlayGroupById(String customerSpace, String playGroupId) {
        String url = constructUrl(PLAY_GROUP_URL_PREFIX + "/{playGroupId}", shortenCustomerSpace(customerSpace),
                playGroupId);
        log.info("url is " + url);
        return get("get Play Group", url, PlayGroup.class);
    }

    public void updatePlayGroup(String customerSpace, String playGroupId, PlayGroup playGroup) {
        String url = constructUrl(PLAY_GROUP_URL_PREFIX + "/{playGroupId}", shortenCustomerSpace(customerSpace),
                playGroupId);
        log.info("url is " + url);
        post("create new Play Groups", url, playGroup, PlayGroup.class);
    }

    public void deletePlayGroupById(String customerSpace, String playGroupId) {
        String url = constructUrl(PLAY_GROUP_URL_PREFIX + "/{playGroupId}", shortenCustomerSpace(customerSpace),
                playGroupId);
        log.info("url is " + url);
        delete("delete Play Group", url);
    }

    public List<PlayLaunchChannel> getPlayLaunchChannels(String customerSpace, String playName,
            Boolean includeUnlaunchedChannels) {
        String url = constructUrl(URL_PREFIX + "/{playName}/channels", shortenCustomerSpace(customerSpace), playName);
        log.info("url is " + url);
        List<String> params = new ArrayList<>();
        params.add("include-unlaunched-channels=" + includeUnlaunchedChannels);
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        return getList("get list of play launch channels", url, PlayLaunchChannel.class);
    }

    public PlayLaunchChannel createPlayLaunchChannel(String customerSpace, String playName,
            PlayLaunchChannel playLaunchChannel) {
        String url = constructUrl(URL_PREFIX + "/{playName}/channels", shortenCustomerSpace(customerSpace), playName);
        log.info("url is " + url);
        return post("create play launch channel", url, playLaunchChannel, PlayLaunchChannel.class);
    }

    public PlayLaunchChannel updatePlayLaunchChannel(String customerSpace, String playName, String channelId,
            PlayLaunchChannel playLaunchChannel) {
        String url = constructUrl(URL_PREFIX + "/{playName}/channels/{channelId}", shortenCustomerSpace(customerSpace),
                playName, channelId);
        log.info("url is " + url);
        return put("updae play launch channel", url, playLaunchChannel, PlayLaunchChannel.class);
    }

    public PlayLaunchChannel launchAlwaysOn(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/launch-always-on", shortenCustomerSpace(customerSpace));
        log.info("url is " + url);
        return post("updae play launch channel", url, null, null);
    }
}
