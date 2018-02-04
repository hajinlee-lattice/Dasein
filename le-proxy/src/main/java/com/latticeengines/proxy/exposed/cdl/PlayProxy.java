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
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("playProxy")
public class PlayProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final Logger log = LoggerFactory.getLogger(PlayProxy.class);

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/plays";

    private static final String DASHBOARD_URL = URL_PREFIX + "/launches/dashboard";

    private static final String DASHBOARD_COUNT_URL = DASHBOARD_URL + "/count";

    public PlayProxy() {
        super("cdl");
    }

    @SuppressWarnings("rawtypes")
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
        List list = get("get plays", url, List.class);
        return JsonUtils.convertList(list, Play.class);
    }

    public PlayLaunchDashboard getPlayLaunchDashboard(String customerSpace, String playName,
            List<LaunchState> launchStates, Long startTimestamp, Long offset, Long max, String sortby,
            Boolean descending, Long endTimestamp) {

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
            params.add("sortby" + sortby);
        }
        if (descending != null) {
            params.add("descending=" + descending);
        }
        if (endTimestamp != null) {
            params.add("end-timestamp=" + endTimestamp);
        }
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        log.info("url is " + url);
        return get("get PlayLaunchDashboard", url, PlayLaunchDashboard.class);
    }

    public Long getPlayLaunchDashboardEntriesCount(String customerSpace, String playName,
            List<LaunchState> launchStates, Long startTimestamp, Long endTimestamp) {
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
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        log.info("url is " + url);
        return get("get PlayLaunch Dashboard Entries Count", url, Long.class);
    }

    public Play getPlay(String customerSpace, String playName) {
        String url = constructUrl(URL_PREFIX + "/{playName}", shortenCustomerSpace(customerSpace), playName);
        log.info("url is " + url);
        return get("get Play", url, Play.class);
    }

    public Play createOrUpdatePlay(String customerSpace, Play play) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        log.info("url is " + url);
        return post("create or update play", url, play, Play.class);
    }

    public void publishTalkingPoints(String customerSpace, String playName) {
        String url = constructUrl(URL_PREFIX + "/{playName}/talkingpoints/publish", shortenCustomerSpace(customerSpace),
                playName);
        log.info("url is " + url);
        post(String.format("publish talking points for play %s", playName), url, null, Void.class);
    }

    public void deletePlay(String customerSpace, String playName) {
        String url = constructUrl(URL_PREFIX + "/{playName}", shortenCustomerSpace(customerSpace), playName);
        log.info("url is " + url);
        delete("Delete a play", url);
    }

    public PlayLaunch createPlayLaunch(String customerSpace, String playName, PlayLaunch playLaunch) {
        String url = constructUrl(URL_PREFIX + "/{playName}/launches", shortenCustomerSpace(customerSpace), playName);
        log.info("url is " + url);
        return post("create or update play", url, playLaunch, PlayLaunch.class);
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
            Double launchCompletionPercent, Long accountsSelected, Long accountsLaunched, Long contactsLaunched,
            Long accountsErrored, Long accountsSuppressed) {
        String url = constructUrl(URL_PREFIX + "/{playName}/launches/{launchId}", shortenCustomerSpace(customerSpace),
                playName, launchId);
        List<String> params = new ArrayList<>();
        if (launchCompletionPercent != null) {
            params.add("launchCompletionPercent=" + launchCompletionPercent);
        }
        if (accountsSelected != null) {
            params.add("accountsSelected=" + accountsSelected);
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

    public void deletePlayLaunch(String customerSpace, String playName, String launchId) {
        String url = constructUrl(URL_PREFIX + "/{playName}/launches/{launchId}", shortenCustomerSpace(customerSpace),
                playName, launchId);
        log.info("url is " + url);
        delete("delete PlayLaunch ", url);
    }

}
