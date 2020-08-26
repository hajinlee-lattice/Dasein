package com.latticeengines.domain.exposed.exception;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TemplateUtils;

/**
 * Only exceptions registered here can be surfaced to UI *
 * Ideally each code here has a corresponding JIRA ticket with designer's requirement
 */
public enum UIActionCode {

    // low level
    SYSTEM_000(Level.System, "Generic error, please contact your system administrator."), // generic system error

    // lookup id
    LOOKUP_ID_01(Level.User, LedpCode.LEDP_40071, "A connection with the credentials you entered already exists. Enter different credentials."), //
    LOOKUP_ID_02(Level.User, LedpCode.LEDP_40080, "System name cannot be empty."), //
    LOOKUP_ID_03(Level.User, LedpCode.LEDP_40081, "A connection with the same system name already exists."), //

    // teams
    TEAMS_00(Level.User, LedpCode.LEDP_18241, "Team name \"${teamName}\" already exists in tenant ${tenantName}."),
    TEAMS_01(Level.User, LedpCode.LEDP_18242, LedpCode.LEDP_18242.getMessage()),

    // orphan records
    ORPHAN_RECORDS_01(Level.User, LedpCode.LEDP_18243, "Could not download result of export job ${exportId}," +
            " download path can't be found, please try to export and download it again."),

    // attribute management
    ATTR_MANAGEMENT_001(Level.User, LedpCode.LEDP_40084, "Failed to add attribute group, exceed size limitation: ${size}."),
    ATTR_MANAGEMENT_002(Level.User, LedpCode.LEDP_40085, LedpCode.LEDP_40085.getMessage()),
    ATTR_MANAGEMENT_003(Level.User, LedpCode.LEDP_40086, "Attribute group name \"${attributeSetName}\" already exists."),
    ATTR_MANAGEMENT_004(Level.User, LedpCode.LEDP_40087, "Can't delete attribute group \"${attributeSetName}\"."),
    ATTR_MANAGEMENT_005(Level.User, LedpCode.LEDP_40094, "This attribute group is used with \"${campaignNames}\" always-on campaign. " +
            "Please make the change to the campaign first and then try to delete the attribute campaign."),

    // file upload
    FILE_UPLOAD_001(Level.User, LedpCode.LEDP_18109, "Problem reading csv file header: ${message}"),
    FILE_UPLOAD_002(Level.User, LedpCode.LEDP_40055),

    // dcp
    DCP_IMPORT_001(Level.User, LedpCode.LEDP_60010, "Upload failed because no records could be ingested successfully"),

    // PA scheduler
    PA_SCHEDULER_001(Level.User, LedpCode.LEDP_40095),

    // UIActionUnitTestNG
    TEST_00(Level.User, LedpCode.LEDP_00009), //
    TEST_01(Level.User, LedpCode.LEDP_00010, "<p>Welcome ${user}!</p>"), //
    TEST_02(Level.User, LedpCode.LEDP_00011), //
    TEST_03(Level.User, LedpCode.LEDP_00012);

    private static final Logger log = LoggerFactory.getLogger(UIActionCode.class);
    private static Map<LedpCode, UIActionCode> ledpCodeMap = new HashMap<>();

    static {
        for (UIActionCode code: UIActionCode.values()) {
            if (code.ledpCode != null) {
                if (ledpCodeMap.containsKey(code.ledpCode)) {
                    log.warn("Duplicated UIActionCode for LedpCode {}", code.ledpCode);
                } else {
                    ledpCodeMap.put(code.ledpCode, code);
                }
            }
        }
    }

    public static UIActionCode fromLedpCode(LedpCode ledpCode) {
        UIActionCode code = ledpCodeMap.get(ledpCode);
        return (code == null) ? SYSTEM_000 : code;
    }

    private final Level level;
    private final LedpCode ledpCode;
    // freemarker template: https://freemarker.apache.org/
    private final String inlineTemplate;

    UIActionCode(Level level, LedpCode ledpCode) {
        this.level = level;
        this.ledpCode = ledpCode;
        this.inlineTemplate = null;
    }

    // allow UIActionCode w/o LedpCode, for
    // 1. mapping multiple LedpCode into one UIActionCode
    // 2. use UIActionCode for non-exception cases
    UIActionCode(Level level, String inlineTemplate) {
        this.level = level;
        this.ledpCode = null;
        this.inlineTemplate = inlineTemplate;
    }

    UIActionCode(Level level, LedpCode ledpCode, String inlineTemplate) {
        this.level = level;
        this.ledpCode = ledpCode;
        this.inlineTemplate = inlineTemplate;
    }

    private String getTemplate() {
        String tpl = "";
        if (StringUtils.isNotBlank(inlineTemplate)) {
            tpl = inlineTemplate;
        } else {
            InputStream is = Thread.currentThread().getContextClassLoader() //
                    .getResourceAsStream("ui_action_templates/" + this.name() + ".ftl");
            if (is != null) {
                try {
                    tpl = IOUtils.toString(is, Charset.defaultCharset());
                } catch (NullPointerException| IOException e) {
                    throw new RuntimeException("Failed to load freemarker template for UIActionCode " + this);
                }
            }
        }
        if (StringUtils.isBlank(tpl) && ledpCode != null) {
            // fall back to ledpCode message;
            tpl = ledpCode.getMessage();
        }
        return tpl;
    }

    String renderMessage(Map<String, Object> params) {
        String tpl = getTemplate();
        if (MapUtils.isNotEmpty(params)) {
            return TemplateUtils.renderByMap(tpl, params);
        } else {
            return TemplateUtils.renderByMap(tpl, Collections.emptyMap());
        }
    }

    String renderMessage(UIActionParams params) {
        String tpl = getTemplate();
        if (params != null) {
            return TemplateUtils.renderByJson(tpl, JsonUtils.convertToJsonNode(params));
        } else {
            return TemplateUtils.renderByMap(tpl, Collections.emptyMap());
        }
    }

    Level getLevel() {
        return level;
    }

    enum Level {
        User, // user can fix the input to resolve the issue.
        System // there is nothing the user can do, must notify support staff
    }

}
