package com.latticeengines.db.dao.cassandra;

import java.io.Serializable;

import org.springframework.data.repository.CrudRepository;

public interface CassandraDao<T, ID extends Serializable> extends CrudRepository<T, ID> {

}
