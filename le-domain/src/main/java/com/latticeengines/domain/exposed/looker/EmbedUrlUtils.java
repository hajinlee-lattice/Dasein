package com.latticeengines.domain.exposed.looker;

import java.math.BigInteger;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public final class EmbedUrlUtils {
    private static final Logger log = LoggerFactory.getLogger(EmbedUrlUtils.class);

    private static final String PATH_FORMAT = "/login/embed/%s";
    private static final String EMBED_URL_FORMAT = "/embed/dashboards-next/%s::%s";
    private static final String VISIT_TABLE_USER_ATTR_NAME = "web_visit_data_table";
    private static final String TARGET_ACCOUNT_LIST_TABLE_USER_ATTR_NAME = "target_account_list_table";

    protected EmbedUrlUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     * Generate Looker SSO embedding URL
     *
     * @param data
     *            information about the dashboard and user
     * @return iframe URL that can be embed in web application
     */
    public static String signEmbedDashboardUrl(@NotNull EmbedUrlData data) {
        validate(data);
        try {
            String path = String.format(PATH_FORMAT, safeUrlEncode(data.getEmbedUrl()));
            String nonce = nonce();
            String timeStr = time();
            String signature = getUrlToSign(data, nonce, timeStr);
            String signedURL = "nonce=" + safeUrlEncode(nonce) + //
                    "&time=" + safeUrlEncode(timeStr) + //
                    "&session_length=" + safeUrlEncode(data.getSessionLength().toString()) + //
                    "&external_user_id=" + safeUrlEncode(JsonUtils.serialize(data.getExternalUserId())) + //
                    "&permissions=" + safeUrlEncode(JsonUtils.serialize(data.getPermissions())) + //
                    "&models=" + safeUrlEncode(JsonUtils.serialize(data.getModels())) + //
                    "&access_filters=" + safeUrlEncode("{}") + //
                    "&signature=" + safeUrlEncode(signature) + //
                    "&first_name=" + safeUrlEncode(JsonUtils.serialize(data.getFirstName())) + //
                    "&last_name=" + safeUrlEncode(JsonUtils.serialize(data.getLastName())) + //
                    "&group_ids=" + safeUrlEncode(JsonUtils.serialize(data.getGroupIds())) + //
                    "&external_group_id=" + safeUrlEncode(JsonUtils.serialize("")) + //
                    "&user_attributes=" + safeUrlEncode(JsonUtils.serialize(data.getUserAttributes())) + //
                    "&force_logout_login=" + safeUrlEncode(JsonUtils.serialize(data.isForceLogoutLogin()));
            return String.format("https://%s%s?%s", data.getHost(), path, signedURL);
        } catch (Exception e) {
            log.error("Failed to sign url data " + data.toString(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Generate embed dashboard relative path
     *
     * @param model
     *            Looker model name
     * @param dashboard
     *            Looker dashboard name
     * @return generated relative path
     */
    public static String embedUrl(@NotNull String model, @NotNull String dashboard) {
        Preconditions.checkArgument(StringUtils.isNotBlank(model), "Model name should not be blank");
        Preconditions.checkArgument(StringUtils.isNotBlank(dashboard), "Dashboard name should not be blank");
        return String.format(EMBED_URL_FORMAT, model, dashboard);
    }

    /**
     * Generate user attribute map given data tables
     *
     * @param visitDataTable
     *            web visit data table name
     * @param targetAccountListTable
     *            target account list table name, nullable
     * @return non-null map of user attribute name -> user attribute value
     */
    public static Map<String, Object> userAttributes(@NotNull String visitDataTable, String targetAccountListTable) {
        Preconditions.checkArgument(StringUtils.isNotBlank(visitDataTable),
                "Visit data table name name should not be blank");
        Map<String, Object> userAttrs = new HashMap<>();
        userAttrs.put(VISIT_TABLE_USER_ATTR_NAME, visitDataTable);
        if (StringUtils.isNotBlank(targetAccountListTable)) {
            userAttrs.put(TARGET_ACCOUNT_LIST_TABLE_USER_ATTR_NAME, targetAccountListTable);
        }
        return userAttrs;
    }

    private static String getUrlToSign(@NotNull EmbedUrlData data, @NotNull String nonce, @NotNull String timeStr)
            throws Exception {
        String path = String.format(PATH_FORMAT, safeUrlEncode(data.getEmbedUrl()));
        String urlToSign = data.getHost() + "\n" + //
                path + "\n" + //
                nonce + "\n" + //
                timeStr + "\n" + //
                data.getSessionLength() + "\n" + //
                JsonUtils.serialize(data.getExternalUserId()) + "\n" + //
                JsonUtils.serialize(data.getPermissions()) + "\n" + //
                JsonUtils.serialize(data.getModels()) + "\n" + //
                JsonUtils.serialize(data.getGroupIds()) + "\n" + //
                JsonUtils.serialize("") + "\n" + // external group id
                JsonUtils.serialize(data.getUserAttributes()) + "\n" + //
                // empty access filter
                "{}";
        return encodeString(urlToSign, data.getSecret());
    }

    private static String nonce() {
        try {
            // force it to re-seed everytime for now
            SecureRandom sr = SecureRandom.getInstance("SHA1PRNG", "SUN");
            sr.nextBytes(new byte[16]);
            // converted to JSON string
            return "\"" + (new BigInteger(130, sr).toString(32)) + "\"";
        } catch (Exception e) {
            log.error("Failed to generate nonce", e);
            throw new RuntimeException(e);
        }
    }

    private static String time() {
        return Long.toString(Instant.now().getEpochSecond());
    }

    private static String safeUrlEncode(@NotNull String url) {
        Preconditions.checkArgument(StringUtils.isNotBlank(url), "URL should not be blank");
        try {
            return URLEncoder.encode(url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String encodeString(String stringToEncode, String secret) throws Exception {
        byte[] keyBytes = secret.getBytes();
        SecretKeySpec signingKey = new SecretKeySpec(keyBytes, "HmacSHA1");
        Mac mac = Mac.getInstance("HmacSHA1");
        mac.init(signingKey);
        byte[] rawHmac = Base64.getEncoder().encode(mac.doFinal(stringToEncode.getBytes(StandardCharsets.UTF_8)));
        return new String(rawHmac, StandardCharsets.UTF_8);
    }

    private static void validate(EmbedUrlData data) {
        Preconditions.checkNotNull(data, "Embedded URL data object should not be null");
        // TODO validation
    }
}
