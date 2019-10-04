package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.Version;

public final class TemplateUtils {

    private static Configuration cfg;

    public static String renderByJson(String template, JsonNode params) {
        return render(template, params);
    }

    public static String renderByMap(String template, Map<String, Object> params) {
        return render(template, params);
    }

    private static String render(String template, Object params) {
        Template tpl;
        try {
            tpl = new Template("inline-template", template, getCfg());
        } catch (IOException e) {
            throw new RuntimeException("Failed to construct a FreeMarker template.", e);
        }
        try {
            StringWriter stringWriter = new StringWriter();
            tpl.process(params, stringWriter);
            return stringWriter.toString();
        } catch (IOException|TemplateException e) {
            String msg = String.format("Failed to render FreeMarker template %s by parameter %s", //
                    template, JsonUtils.serialize(params));
            throw new RuntimeException(msg, e);
        }
    }

    private static Configuration getCfg() {
        if (cfg == null) {
            configure();
        }
        return cfg;
    }

    private static synchronized void configure() {
        if (cfg == null) {
            Configuration cfg = new Configuration(new Version(2, 3, 29));
            cfg.setDefaultEncoding("UTF-8");
            cfg.setLocale(Locale.US);
            cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
            TemplateUtils.cfg = cfg;
        }
    }

}
