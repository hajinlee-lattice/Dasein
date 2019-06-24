package com.latticeengines.domain.exposed.pls;

import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class PlayLaunchSparkContext {

    private static final String DEFAULT_JOIN_KEY = InterfaceName.AccountId.name();

    @JsonProperty("JoinKey")
    private String joinKey;

    @JsonProperty("Tenant")
    private Tenant tenant;

    @JsonProperty("PlayName")
    private String playName;

    @JsonProperty("PlayLaunchId")
    private String playLaunchId;

    @JsonProperty("PlayLaunch")
    private PlayLaunch playLaunch;

    @JsonProperty("Play")
    private Play play;

    @JsonProperty("LaunchTimestampMillis")
    private long launchTimestampMillis;

    @JsonProperty("RatingId")
    private String ratingId;

    @JsonProperty("PublishedIteration")
    private RatingModel publishedIteration;

    @JsonProperty("Counter")
    private Counter counter;

    public PlayLaunchSparkContext() {
    }

    public PlayLaunchSparkContext(Tenant tenant, String playName, String playLaunchId, PlayLaunch playLaunch, Play play,
            long launchTimestampMillis, String ratingId, RatingModel publishedIteration, Counter counter) {
        super();
        this.joinKey = DEFAULT_JOIN_KEY;
        this.tenant = tenant;
        this.playName = playName;
        this.playLaunchId = playLaunchId;
        this.playLaunch = playLaunch;
        this.play = play;
        this.launchTimestampMillis = launchTimestampMillis;
        this.ratingId = ratingId;
        this.publishedIteration = publishedIteration;
        this.counter = counter;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public String getPlayName() {
        return playName;
    }

    public String getPlayLaunchId() {
        return playLaunchId;
    }

    public PlayLaunch getPlayLaunch() {
        return playLaunch;
    }

    public Play getPlay() {
        return play;
    }

    public long getLaunchTimestampMillis() {
        return launchTimestampMillis;
    }

    public RatingModel getPublishedIteration() {
        return publishedIteration;
    }

    public String getRatingId() {
        return ratingId;
    }

    public static class Counter {
        private AtomicLong accountLaunched;
        private AtomicLong contactLaunched;
        private AtomicLong accountErrored;
        private AtomicLong contactErrored;

        public Counter() {
            accountLaunched = new AtomicLong();
            contactLaunched = new AtomicLong();
            accountErrored = new AtomicLong();
            contactErrored = new AtomicLong();
        }

        public AtomicLong getAccountLaunched() {
            return accountLaunched;
        }

        public AtomicLong getContactLaunched() {
            return contactLaunched;
        }

        public AtomicLong getAccountErrored() {
            return accountErrored;
        }

        public AtomicLong getContactErrored() {
            return contactErrored;
        }
    }

}
