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

    public Class returnedClass() {
        return Double.class;
    }

    public boolean equals(Object x, Object y) throws HibernateException {
        return (x == y) || (x != null && x.equals(y));
    }

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

    public Object deepCopy(Object value) throws HibernateException {
        // returning value should be OK since doubles are immutable
        return value;
    }

    public boolean isMutable() {
        return false;
    }

    public Serializable disassemble(Object value) throws HibernateException {
        return (Serializable) value;
    }

    public Object assemble(Serializable cached, Object owner) throws HibernateException {
        return cached;
    }

    public Object replace(Object original, Object target, Object owner) throws HibernateException {
        return original;
    }
}
