package com.latticeengines.dataplatform.dao.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.exposed.domain.HasId;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;

public abstract class BaseDaoImpl<T extends HasId<?>> implements BaseDao<T> {

    private PropertiesConfiguration store = null;
    
    BaseDaoImpl() {
        String fileName = getFileName();
        File f = new File(fileName);
        
        try {
            f.createNewFile();
            store = new PropertiesConfiguration(fileName);
            store.setDelimiterParsingDisabled(true);
            store.setListDelimiter((char) 0);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_14000, e, new String[] { fileName });
        } 
    }
    
    protected String getFileName() {
        return getClass().getName() + ".properties";
    }
    
    protected PropertiesConfiguration getStore() {
        return store;
    }
    
    @Override
    public void load() {
        String fileName = getFileName();
        try {
            store.load(fileName);
        } catch (ConfigurationException e) {
            throw new LedpException(LedpCode.LEDP_14001, e, new String[] { fileName });
        }
    }
    
    @Override
    public void save() {
        try {
            store.save();
        } catch (ConfigurationException e) {
            throw new LedpException(LedpCode.LEDP_14002, e, new String[] { getFileName() });
        }
    }

    @Override
    public void post(T entity) {
        store.setProperty(entity.getId().toString(), serialize(entity));
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public T getById(String id) {
        return deserialize(id, (String) store.getProperty(id));
    }
    
    @Override
    public void deleteStoreFile() {
        clear();
        File f = new File(getFileName());
        f.delete();
    }

    @Override
    public String serialize(T entity) {
        return entity.toString();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public List<T> getAll() {
        List<T> values = new ArrayList<T>();
        for (Iterator<String> it = (Iterator<String>) store.getKeys(); it.hasNext();) {
            String key = it.next();
            Object value = store.getProperty(key);
            if (value instanceof List) {
                value = (String) StringUtils.join((List<String>) value, ",");
            }
            if (value != null) {
                values.add(deserialize(key, (String) value));
            }
            
        }
        return values;
    }
    
}
