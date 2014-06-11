package com.latticeengines.dataplatform.dao.impl;

 
import java.util.NoSuchElementException;

import com.latticeengines.dataplatform.dao.SequenceDao;

/// @Component("sequenceDao")

public class SequenceDaoImpl extends BaseDaoImpl implements SequenceDao {
    
    public SequenceDaoImpl() {
        super();
        getStore().setAutoSave(true);
    }

    
    public Sequence deserialize(String id, String content) {
        return new Sequence(Long.parseLong(content));
    }

    
    public String serialize(Sequence sequence) {
        return Long.toString(sequence.getId());
    }

    @Override
    public synchronized Long nextVal(String key) {
        Long value = null;
        try {
            value = getStore().getLong(key);
        } catch (NoSuchElementException e) {
            value = 0L;
        }
        value = value + 1;
        getStore().setProperty(key, value);
        return value;
    }


   


    @Override
    protected Class getEntityClass() {
        // TODO Auto-generated method stub
        return null;
    }

}
