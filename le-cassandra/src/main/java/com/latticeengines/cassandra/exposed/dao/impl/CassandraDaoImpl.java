package com.latticeengines.cassandra.exposed.dao.impl;

import java.io.Serializable;

import org.springframework.data.cassandra.core.CassandraOperations;

import com.latticeengines.cassandra.exposed.dao.CassandraDao;

public class CassandraDaoImpl<T, ID extends Serializable> implements CassandraDao<T, ID> {

    private Class<T> type;
    private CassandraOperations template;

    public CassandraDaoImpl(CassandraOperations template) {
        this.template = template;
    }

    public CassandraDaoImpl(CassandraOperations template, Class<T> type) {
        this.template = template;
        this.type = type;
    }

    @Override
    public <S extends T> S save(S entity) {
        return template.insert(entity);
    }

    @Override
    public <S extends T> Iterable<S> save(Iterable<S> entities) {
        return template.insert(entities);
    }

    @Override
    public T findOne(ID id) {
        Class<T> type = null;
        return template.selectOneById(type, id);
    }

    @Override
    public boolean exists(ID id) {
        return template.exists(type, id);
    }

    @Override
    public Iterable<T> findAll() {
        return template.selectAll(type);
    }

    @Override
    public Iterable<T> findAll(Iterable<ID> ids) {
        return template.selectBySimpleIds(type, ids);
    }

    @Override
    public long count() {
        return template.count(type);
    }

    @Override
    public void delete(ID id) {
        template.deleteById(type, id);
    }

    @Override
    public void delete(T entity) {
        template.delete(entity);
    }

    @Override
    public void delete(Iterable<? extends T> entities) {
        template.delete(entities);
    }

    @Override
    public void deleteAll() {
        template.deleteAll(type);
    }

}
