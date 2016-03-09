package com.latticeengines.db.exposed.extension;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.usertype.UserType;

public class NaNSafeDoubleType implements UserType {
    public int[] sqlTypes() {
        return new int[] { Types.DOUBLE };
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class returnedClass() {
        return Double.class;
    }

    @Override
    public boolean equals(Object x, Object y) throws HibernateException {
        return (x == y) || (x != null && x.equals(y));
    }

    @Override
    public int hashCode(Object x) throws HibernateException {
        return x.hashCode();
    }

    @Override
    public Object nullSafeGet(ResultSet rs, String[] names, SessionImplementor session, Object owner)
            throws HibernateException, SQLException {
        double value = rs.getDouble(names[0]);
        if (rs.wasNull()) {
            value = Double.NaN;
        }
        return value;
    }

    @Override
    public void nullSafeSet(PreparedStatement st, Object value, int index, SessionImplementor session)
            throws HibernateException, SQLException {
        if (value == null || Double.isNaN((Double) value)) {
            st.setNull(index, Types.DOUBLE);
        } else {
            st.setDouble(index, ((Double) value));
        }
    }

    @Override
    public Object deepCopy(Object value) throws HibernateException {
        // returning value should be OK since doubles are immutable
        return value;
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public Serializable disassemble(Object value) throws HibernateException {
        return (Serializable) value;
    }

    @Override
    public Object assemble(Serializable cached, Object owner) throws HibernateException {
        return cached;
    }

    @Override
    public Object replace(Object original, Object target, Object owner) throws HibernateException {
        return original;
    }
}
