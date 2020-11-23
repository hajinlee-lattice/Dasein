package com.latticeengines.domain.exposed.cdl.activity;

/**
 * class to hold constants for all features built on top of this time series
 * processing framework (activity store)
 */
public final class ActivityStoreConstants {

    private ActivityStoreConstants() {
    }

    public static class DnbIntent {
        public static final String STAGE_BUYING = "Buying";
        public static final String STAGE_RESEARCHING = "Researching";
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
        public static final String ANONYMOUS_WEB_VISITS = "AnonymousWebVisits";
        public static final String RE_ENGAGED_ACTIVITY = "ReEngagedActivity";
        public static final String SHOWN_INTENT = "HasShownIntent";
        public static final String HIGH_ENGAGEMENT_IN_ACCOUNT = "HighEngagementInAccount";
        public static final String KNOWN_WEB_VISITS = "KnownWebVisits";
        public static final String ACTIVE_CONTACT_WEB_VISITS = "AcitiveContactsAndWebVisits";
        public static final String BUYING_INTENT_AROUND_PRODUCT_PAGES = "BuyingIntentAroundProductPages";
        public static final String RESEARCHING_INTENT_AROUND_PRODUCT_PAGES = "ResearchingIntentAroundProductPages";

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
        public static final String COL_TOTAL_MA_COUNTS = "TotalMaCounts";
        public static final String COL_TITLES = "Titles";
        public static final String COL_TITLE_CNT = "TitleCount";
        public static final String COL_STAGE = "IntentStage";

        public static final long RE_ENGAGED_QUIET_PERIOD_IN_DAYS = 30L;
    }
}
