package com.latticeengines.domain.exposed.cdl.activity;

/**
 * class to hold constants for all features built on top of this time series
 * processing framework (activity store)
 */
public final class ActivityStoreConstants {

    private ActivityStoreConstants() {
    }

    public static class DnbIntent {
        public static final String BUYING_STAGE = "Buying";
        public static final String RESEARCHING_STAGE = "Researching";
        public static final double BUYING_STAGE_THRESHOLD = 0.5;
    }

    public static class JourneyStage {
        public static final String STREAM_SOURCE_ATLAS = "Atlas";
        public static final String STREAM_DETAIL1_DARK = "Dark";
        public static final String STREAM_EVENT_TYPE_JOURNEYSTAGECHANGE = "Journey Stage Change";
    }

    /**
     * {@link ActivityAlertsConfig} related constants
     */
    public static class Alert {
        /**
         * {@link ActivityAlertsConfig#getName()}
         */
        public static final String INC_WEB_ACTIVITY = "IncWebActivity";
        public static final String INC_WEB_ACTIVITY_ON_PRODUCT = "IncActivityOnProduct";
        public static final String RE_ENGAGED_ACTIVITY = "ReEngagedActivity";
        public static final String SHOWN_INTENT = "HasShownIntent";

        public static final String COL_ALERT_DATA = "Data";
        public static final String COL_START_TIMESTAMP = "StartTimestamp";
        public static final String COL_END_TIMESTAMP = "EndTimestamp";
        public static final String COL_PAGE_VISITS = "PageVisits";
        public static final String COL_PAGE_NAME = "PageName";
        public static final String COL_ACTIVE_CONTACTS = "ActiveContacts";
        public static final String COL_PAGE_VISIT_TIME = "PageVisitTime";
        public static final String COL_PREV_PAGE_VISIT_TIME = "PrevPageVisitTime";
        public static final String COL_NUM_RESEARCH_INTENTS = "NumResearchIntents";
        public static final String COL_NUM_BUY_INTENTS = "NumBuyIntents";
        public static final String COL_RE_ENGAGED_CONTACTS = "ReEngagedContacts";

        public static final long RE_ENGAGED_QUIET_PERIOD_IN_DAYS = 30L;
    }
}
