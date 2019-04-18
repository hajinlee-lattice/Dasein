package com.latticeengines.common.exposed.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

public class PropertyUtils extends PropertyPlaceholderConfigurer {
    private static Logger log = LoggerFactory.getLogger(PropertyUtils.class);

    private static Map<String, String> propertiesMap;

    private int springSystemPropertiesMode = SYSTEM_PROPERTIES_MODE_FALLBACK;

    @Override
    public void setSystemPropertiesMode(int systemPropertiesMode) {
        super.setSystemPropertiesMode(systemPropertiesMode);
        springSystemPropertiesMode = systemPropertiesMode;
    }

    @Override
    protected void processProperties(ConfigurableListableBeanFactory beanFactory, Properties props)
            throws BeansException {
        propertiesMap = new HashMap<>();

        if (log.isDebugEnabled()) {
            log.debug("Loading properties");
            for (String key : props.stringPropertyNames()) {
                log.debug(String.format("%s: %s", key, props.getProperty(key)));
            }
        }
        // Need to replace variables because there's no convenient way to get
        // Spring to do it
        Pattern pattern = Pattern.compile("(\\$\\{(\\w+)\\})");
        for (Object key : props.keySet()) {
            String value = (String) props.get(key);
            if (value.contains("$")) {
                while (true) {
                    Matcher matcher = pattern.matcher(value);
                    if (!matcher.find()) {
                        break;
                    }
                    String placeholder = matcher.group(2);
                    String replacement = resolvePlaceholder(placeholder, props, springSystemPropertiesMode);
                    if (replacement == null) {
                        replacement = "";
                    }
                    value = matcher.replaceFirst(replacement);
                    log.debug(String.format("%s: %s", key, value));
                }
            }
            props.put(key, value);
            propertiesMap.put((String) key, value);
        }
        for (Object key : props.keySet()) {
            String keyStr = key.toString();
            String valueStr = resolvePlaceholder(keyStr, props, springSystemPropertiesMode);
            // Decrypt credentials
            if (keyStr.contains(CipherUtils.ENCRYPTED)) {
                try {
                    valueStr = CipherUtils.decrypt(valueStr);
                } catch (Exception e) {
                    throw new RuntimeException("Decryption failed when parsing property " + keyStr + ":" + valueStr, e);
                }
                props.put(keyStr, valueStr);
            }
            propertiesMap.put(keyStr, valueStr);
        }
        super.processProperties(beanFactory, props);

    }

    public static String getProperty(String name) {
        if (MapUtils.isEmpty(propertiesMap)) {
            return null;
        } else {
            return propertiesMap.get(name);
        }
    }

}
