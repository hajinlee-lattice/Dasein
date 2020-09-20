package com.latticeengines.domain.exposed.datacloud.match;

import java.text.SimpleDateFormat;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * https://confluence.lattice-engines.com/display/DCP/Usage+Reporting
 */
public final class VboUsageConstants {

    protected VboUsageConstants() {
        throw new UnsupportedOperationException();
    }

    public static final String EVENT_MATCH = "Match";
    public static final String EVENT_DATA = "Data";

    public static final String FEATURE_MATCH = "clnmat";

    private static final String ISO_8601 = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    public static final SimpleDateFormat USAGE_EVENT_TIME_FORMAT = new SimpleDateFormat(ISO_8601);

    // tracked in match engine
    public static final String ATTR_POAEID = "POAEID";
    public static final String ATTR_TIMESTAMP = "TimeStamp";
    public static final String ATTR_RESPONSE_TIME = "Response Time";
    public static final String ATTR_EVENT_TYPE = "Event Type";
    public static final String ATTR_FEATURE = "Feature URI";
    public static final String ATTR_SUBJECT_DUNS = "Subject DUNS/Entity ID";
    public static final String ATTR_SUBJECT_NAME = "Subject Name";
    public static final String ATTR_SUBJECT_CITY = "Subject City";
    public static final String ATTR_SUBJECT_STATE = "Subject State/Province";
    public static final String ATTR_SUBJECT_COUNTRY = "Subject Country";

    // match engine needs avro friendly attribute names
    public static final String AVRO_ATTR_POAEID = "poaeId";
    public static final String AVRO_ATTR_TIMESTAMP = "timestamp";
    public static final String AVRO_ATTR_RESPONSE_TIME = "responseTime";
    public static final String AVRO_ATTR_EVENT_TYPE = "eventType";
    public static final String AVRO_ATTR_FEATURE = "featureUri";
    public static final String AVRO_ATTR_SUBJECT_DUNS = "subjectDuns";
    public static final String AVRO_ATTR_SUBJECT_NAME = "subjectName";
    public static final String AVRO_ATTR_SUBJECT_CITY = "subjectCity";
    public static final String AVRO_ATTR_SUBJECT_STATE = "subjectState";
    public static final String AVRO_ATTR_SUBJECT_COUNTRY = "subjectCountry";

    // to be enriched post match
    public static final String ATTR_DRT = "DRT";
    public static final String ATTR_DELIVERY_CHANNEL = "Delivery Channel";
    public static final String ATTR_DELIVERY_MODE = "Delivery Mode";
    public static final String ATTR_LEID = "LEID";
    public static final String ATTR_SUBSCRIBER_NUMBER = "Subscriber Number";
    public static final String ATTR_SUBSCRIBER_NAME = "Subscriber Name";
    public static final String ATTR_SUBSCRIBER_COUNTRY = "Subscriber Country";
    public static final String ATTR_APPID = "APPID";
    public static final String ATTR_CAPPID = "CAPPID";

    // not applicable for now
    public static final String ATTR_GUID = "GUID";
    public static final String ATTR_CLIENT_ID = "Client ID (API Key)";
    public static final String ATTR_KEY_TYPE = "API Key Type";
    public static final String ATTR_EMAIL = "EMAIL ADDRESS";
    public static final String ATTR_LUID = "LUID";
    public static final String ATTR_AGENT_ID = "Agent ID";
    public static final String ATTR_CONSUMER_IP = "Consumer IP";
    public static final String ATTR_REASON_CODE = "Reason Code";
    public static final String ATTR_CUSTOMER_REFERENCE = "Customer Reference";
    public static final String ATTR_PORTFOLIO_SIZE = "Portfolio Size";
    public static final String ATTR_USER_LOCATION = "User Location";
    public static final String ATTR_CONTRACT_ID = "Contract ID";
    public static final String ATTR_CONTRACT_START = "Contract Term Start Date";
    public static final String ATTR_CONTRACT_END = "Contract Term End Date";
    public static final String ATTR_PRICING_REGION = "Pricing (Subscriber) Region";
    public static final String ATTR_RECORD_REGION = "Record (Subject Country) Region";
    public static final String ATTR_GO_NUMBER = "GO Number";
    public static final String ATTR_GLOBAL_SUBSCRIBER = "Global Subscriber Number";
    public static final String ATTR_PURCHASED_METER = "Purchased Meter/RUM Value";
    public static final String ATTR_AVAILABLE_METER = "Available Meter/RUM Value";
    public static final String ATTR_USAGE_PRICE = "Usage Price";
    public static final String ATTR_CURRENCY = "Currency";
    public static final String ATTR_BOOK_PRICE = "Book Price";
    public static final String ATTR_TAXABLE = "Taxable (True/False)";
    public static final String ATTR_ERROR_CODE = "Error Code";

    // raw usage schema for usage avro generated in bulk match
    public static final ImmutableList<Pair<String, Class<?>>> RAW_USAGE_SCHEMA = //
            ImmutableList.<Pair<String, Class<?>>>builder()
                    .add(Pair.of(AVRO_ATTR_POAEID, String.class)) //
                    .add(Pair.of(AVRO_ATTR_TIMESTAMP, String.class)) //
                    .add(Pair.of(AVRO_ATTR_RESPONSE_TIME, Long.class)) //
                    .add(Pair.of(AVRO_ATTR_EVENT_TYPE, String.class)) //
                    .add(Pair.of(AVRO_ATTR_FEATURE, String.class)) //
                    .add(Pair.of(AVRO_ATTR_SUBJECT_DUNS, String.class)) //
                    .add(Pair.of(AVRO_ATTR_SUBJECT_NAME, String.class)) //
                    .add(Pair.of(AVRO_ATTR_SUBJECT_CITY, String.class)) //
                    .add(Pair.of(AVRO_ATTR_SUBJECT_STATE, String.class)) //
                    .add(Pair.of(AVRO_ATTR_SUBJECT_COUNTRY, String.class)) //
                    .build();

    // use this dictionary to map avro attribute to csv attribute, for usage data generated by bulk match
    // (avro attr) -> (csv attr)
    public static final ImmutableMap<String, String> RAW_USAGE_DISPLAY_NAMES = ImmutableMap.<String, String>builder() //
            .put(AVRO_ATTR_POAEID, ATTR_POAEID) //
            .put(AVRO_ATTR_TIMESTAMP, ATTR_TIMESTAMP) //
            .put(AVRO_ATTR_RESPONSE_TIME, ATTR_RESPONSE_TIME) //
            .put(AVRO_ATTR_EVENT_TYPE, ATTR_EVENT_TYPE) //
            .put(AVRO_ATTR_FEATURE, ATTR_FEATURE) //
            .put(AVRO_ATTR_SUBJECT_DUNS, ATTR_SUBJECT_DUNS) //
            .put(AVRO_ATTR_SUBJECT_NAME, ATTR_SUBJECT_NAME) //
            .put(AVRO_ATTR_SUBJECT_CITY, ATTR_SUBJECT_CITY) //
            .put(AVRO_ATTR_SUBJECT_STATE, ATTR_SUBJECT_STATE) //
            .put(AVRO_ATTR_SUBJECT_COUNTRY, ATTR_SUBJECT_COUNTRY) //
            .build();

    // the final csv send to VBO must use the headers in this exact order
    public static final ImmutableList<String> OUTPUT_FIELDS = ImmutableList.<String>builder() //
            .add(ATTR_GUID) //
            .add(ATTR_CLIENT_ID) //
            .add(ATTR_KEY_TYPE) //
            .add(ATTR_DRT) //
            .add(ATTR_DELIVERY_MODE) //
            .add(ATTR_DELIVERY_CHANNEL) //
            .add(ATTR_EMAIL) //
            .add(ATTR_LEID) //
            .add(ATTR_LUID) //
            .add(ATTR_POAEID) //
            .add(ATTR_SUBSCRIBER_NUMBER) //
            .add(ATTR_SUBSCRIBER_NAME) //
            .add(ATTR_AGENT_ID) //
            .add(ATTR_APPID) //
            .add(ATTR_CAPPID) //
            .add(ATTR_EVENT_TYPE) //
            .add(ATTR_TIMESTAMP) //
            .add(ATTR_FEATURE) //
            .add(ATTR_CONSUMER_IP) //
            .add(ATTR_SUBJECT_DUNS) //
            .add(ATTR_REASON_CODE) //
            .add(ATTR_CUSTOMER_REFERENCE) //
            .add(ATTR_RESPONSE_TIME) //
            .add(ATTR_PORTFOLIO_SIZE) //
            .add(ATTR_SUBSCRIBER_COUNTRY) //
            .add(ATTR_USER_LOCATION) //
            .add(ATTR_SUBJECT_NAME) //
            .add(ATTR_SUBJECT_CITY) //
            .add(ATTR_SUBJECT_STATE) //
            .add(ATTR_SUBJECT_COUNTRY) //
            .add(ATTR_CONTRACT_ID) //
            .add(ATTR_CONTRACT_START) //
            .add(ATTR_CONTRACT_END) //
            .add(ATTR_PRICING_REGION) //
            .add(ATTR_RECORD_REGION) //
            .add(ATTR_GO_NUMBER) //
            .add(ATTR_GLOBAL_SUBSCRIBER) //
            .add(ATTR_PURCHASED_METER) //
            .add(ATTR_AVAILABLE_METER) //
            .add(ATTR_USAGE_PRICE) //
            .add(ATTR_CURRENCY) //
            .add(ATTR_BOOK_PRICE) //
            .add(ATTR_TAXABLE) //
            .add(ATTR_ERROR_CODE) //
            .build();

}

