package com.latticeengines.camille.exposed.watchers;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;

public class DebugGatewayWatcher {

    private static final Logger log = LoggerFactory.getLogger(DebugGatewayWatcher.class);

    private static final String WATCHER_NAME = "DebugGateway";
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static final Pattern SIGNAL_PATTERN = Pattern.compile("^(?<tenantId>[^\\|]+)\\|(?<duration>.*)$");
    private static final ConcurrentMap<String, Long> GATEWAY_PASSPORTS = new ConcurrentHashMap<>();

    public static boolean hasPassport(String tenantId) {
        boolean hasPassport = false;
        if (GATEWAY_PASSPORTS.containsKey(tenantId)) {
            Long expiration = GATEWAY_PASSPORTS.get(tenantId);
            if (System.currentTimeMillis() >= expiration) {
                log.warn(String.format("%s's passport to debug gateway has expired.", tenantId));
                GATEWAY_PASSPORTS.remove(tenantId);
            } else {
                hasPassport = true;
            }
        }
        return hasPassport;
    }

    public static void applyForPassportAsync(String tenantId, String duration) {
        if (StringUtils.isNotBlank(tenantId) && StringUtils.isNotBlank(duration)) {
            try {
                Duration.parse(duration);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse duration");
            }
            NodeWatcher.updateWatchedData(WATCHER_NAME, tenantId + "|" + duration);
        } else {
            throw new RuntimeException("Must provide valid tenantId and duration.");
        }
    }

    public static void initialize() {
        if (!INITIALIZED.get()) {
            synchronized (DebugGatewayWatcher.class) {
                if (!INITIALIZED.get()) {
                    NodeWatcher.registerWatcher(WATCHER_NAME);
                    NodeWatcher.registerListener(WATCHER_NAME, () -> {
                        String data = NodeWatcher.getWatchedData(WATCHER_NAME);
                        log.debug("Received watcher data: " + data);
                        if (StringUtils.isNotBlank(data)) {
                            issuePassport(data);
                            NodeWatcher.updateWatchedData(WATCHER_NAME, "");
                        }
                    });
                    INITIALIZED.set(true);
                }
            }
        }
    }

    private static void issuePassport(String signal) {
        try {
            Matcher matcher = SIGNAL_PATTERN.matcher(signal);
            if (matcher.matches()) {
                String tenantId = matcher.group("tenantId");
                String duration = matcher.group("duration");
                Duration javaDuration = Duration.parse(duration);
                String durationStr = formatDuration(javaDuration);
                Long expiration = System.currentTimeMillis() + javaDuration.toMillis();
                GATEWAY_PASSPORTS.put(tenantId, expiration);
                log.warn(String.format("Issued a gateway passport to %s for %s (%s)", //
                        tenantId, durationStr, duration));
            } else {
                log.warn("Invalid signal: " + signal);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to issue debug gateway passport to " //
                    + JsonUtils.serialize(signal), e);
        }
    }

    private static String formatDuration(Duration duration) {
        long seconds = duration.getSeconds();
        long absSeconds = Math.abs(seconds);
        long h = absSeconds / 3600;
        long m = (absSeconds % 3600) / 60;
        long s = absSeconds % 60;
        String positive = "";
        if (h > 0) {
            positive += String.format("%d h", h);
        }
        if (m > 0) {
            positive += String.format("%02d m", h);
        }
        positive += String.format("%02d s", s);
        return seconds < 0 ? "-" + positive : positive;
    }

}
