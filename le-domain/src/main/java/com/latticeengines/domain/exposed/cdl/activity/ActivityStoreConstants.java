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
        public static final String SHOWN_BUYER_INTENT = "HasShownBuyIntent";
        public static final String SHOWN_RESEARCH_INTENT = "HasShownResearchIntent";

        public static final String COL_ALERT_DATA = "alert";
        public static final String COL_START_TIMESTAMP = "start_timestamp";
        public static final String COL_END_TIMESTAMP = "end_timestamp";
        public static final String COL_PAGE_VISITS = "page_visits";
        public static final String COL_PAGE_NAME = "page_name";
        public static final String COL_MODEL_NAME = "model_name";
        public static final String COL_ACTIVE_CONTACTS = "active_contacts";
        public static final String COL_PAGE_VISIT_TIME = "page_visit_time";
        public static final String COL_PREV_PAGE_VISIT_TIME = "prev_page_visit_time";

        public static final long RE_ENGAGED_QUIET_PERIOD_IN_DAYS = 30L;
    }
}
