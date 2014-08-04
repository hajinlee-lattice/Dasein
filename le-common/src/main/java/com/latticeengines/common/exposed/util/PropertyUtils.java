package com.latticeengines.common.exposed.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

public class PropertyUtils extends PropertyPlaceholderConfigurer {

    private static Map<String, String> propertiesMap;

    private int springSystemPropertiesMode = SYSTEM_PROPERTIES_MODE_FALLBACK;

    @Override
    public void setSystemPropertiesMode(int systemPropertiesMode) {
        super.setSystemPropertiesMode(systemPropertiesMode);
        springSystemPropertiesMode = systemPropertiesMode;
    }

    @Override
    protected void processProperties(
            ConfigurableListableBeanFactory beanFactory, Properties props)
            throws BeansException {

        propertiesMap = new HashMap<String, String>();
        for (Object key : props.keySet()) {
            String keyStr = key.toString();
            String valueStr = resolvePlaceholder(keyStr, props,
                    springSystemPropertiesMode);
            // Decrypt credentials
            if (keyStr.contains(CipherUtils.ENCRYPTED)) {
                try {
                    valueStr = CipherUtils.decrypt(valueStr);
                } catch (Exception e) {
                    throw new RuntimeException("Decryption failed when parsing properties.", e);
                }
                props.put(keyStr, valueStr);
            }
            propertiesMap.put(keyStr, valueStr);
        }
        
        super.processProperties(beanFactory, props);
    }

    public static String getProperty(String name) {
        return propertiesMap.get(name).toString();
    }

}