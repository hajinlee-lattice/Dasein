package com.latticeengines.domain.exposed.dataplatform.hibernate;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.type.AbstractSingleColumnStandardBasicType;
import org.hibernate.type.AbstractStandardBasicType;
import org.hibernate.type.TypeFactory;
import org.hibernate.type.TypeResolver;
import org.hibernate.type.spi.TypeConfiguration;
import org.hibernate.usertype.ParameterizedType;
import org.hibernate.usertype.UserType;

public class GenericEnumUserType<E extends Enum<E>> implements UserType, ParameterizedType {
    public static final String PROPERTY_NAME_IDENTIFIER_METHOD = "identifierMethod";
    public static final String PROPERTY_NAME_ENUM_CLASS_NAME = "enumClassName";

    private static final Object PROPERTY_NAME_VALUE_OF_METHOD = "valueOfMethod";
    private static final String DEFAULT_IDENTIFIER_METHOD_NAME = "name";
    private static final String DEFAULT_VALUE_OF_METHOD_NAME = "valueOf";

    protected Class<?> enumClass;
    protected Class<?> identifierType;
    protected Method identifierMethod;
    protected Method valueOfMethod;
    // private NullableType type;
    protected AbstractStandardBasicType<?> type;

    private int[] sqlTypes;

    /**
     * Constructor.
     *
     * @param clazz
     */
    public GenericEnumUserType(Class<E> clazz) {
        enumClass = clazz;
    }

    /**
     * Called by hibernate with all propertiies given as params in a given
     * mapping. Valid values for this class are: enumClassName,
     * identifierMethod, and valueOfMethod, as described in the class preamble.
     */
    @SuppressWarnings({ "unchecked" })
    @Override
    public void setParameterValues(Properties parameters) {

        // enumClassName
        if (!parameters.containsKey(PROPERTY_NAME_ENUM_CLASS_NAME)) {
            throw new HibernateException(" #100000001 : The property '"
                    + PROPERTY_NAME_ENUM_CLASS_NAME + "' not found!");
        }
        String enumClassName = parameters.getProperty(PROPERTY_NAME_ENUM_CLASS_NAME);

        try {
            enumClass = Class.forName(enumClassName).asSubclass(Enum.class);

        } catch (ClassNotFoundException cfne) {
            throw new HibernateException("Enum class given by property '"
                    + PROPERTY_NAME_ENUM_CLASS_NAME + "' not found", cfne);
        }

        // identifierMethod
        if (!parameters.containsKey(PROPERTY_NAME_IDENTIFIER_METHOD)) {
            throw new HibernateException(" #100000002 : The property '"
                    + PROPERTY_NAME_IDENTIFIER_METHOD + "' not found!");
        }
        String identifierMethodName = parameters.getProperty(PROPERTY_NAME_IDENTIFIER_METHOD,
                DEFAULT_IDENTIFIER_METHOD_NAME);

        try {
            identifierMethod = enumClass.getMethod(identifierMethodName, new Class[0]);
            identifierType = identifierMethod.getReturnType();
        } catch (Exception e) {
            throw new HibernateException("Failed to obtain identifier method named '"
                    + identifierMethodName + "' in class '" + enumClass.getName()
                    + "' given by property '" + PROPERTY_NAME_IDENTIFIER_METHOD + "'", e);
        }

        //FIXME: TypeResolver is deprecated, according to Hibernate
        // (since 5.3) No replacement, access to and handling of Types will be much different in 6.0
        // https://docs.jboss.org/hibernate/orm/5.3/javadocs/deprecated-list.html
        final TypeConfiguration tc = new TypeConfiguration();
        final TypeResolver tr = new TypeResolver(tc, new TypeFactory(tc));
        type = (AbstractSingleColumnStandardBasicType<? extends Object>)
                tr.heuristicType(identifierType.getName(), parameters);

        if (type == null)
            throw new HibernateException("Unsupported identifier type " + identifierType.getName());

        sqlTypes = new int[] { ((AbstractSingleColumnStandardBasicType<?>) type).sqlType() };

        // valueOfMethod
        if (!parameters.containsKey(PROPERTY_NAME_VALUE_OF_METHOD)) {
            throw new HibernateException(" #100000003 : The property '"
                    + PROPERTY_NAME_VALUE_OF_METHOD + "' not found!");
        }
        String valueOfMethodName = parameters.getProperty("valueOfMethod",
                DEFAULT_VALUE_OF_METHOD_NAME);

        try {
            valueOfMethod = enumClass.getMethod(valueOfMethodName, new Class[] { identifierType });
        } catch (Exception e) {
            throw new HibernateException("Failed to obtain a method named '" + valueOfMethodName
                    + "', which accepts an argument of type '" + identifierType + "' in class '"
                    + enumClass.getName() + "' for property '" + PROPERTY_NAME_VALUE_OF_METHOD
                    + "'", e);
        }
    }

    @Override
    public Class<?> returnedClass() {
        return enumClass;
    }

    @Override
    public int[] sqlTypes() {
        return sqlTypes;
    }

    @Override
    public Object assemble(Serializable cached, Object owner) throws HibernateException {
        return cached;
    }

    @Override
    public Object deepCopy(Object value) throws HibernateException {
        return value;
    }

    @Override
    public Serializable disassemble(Object value) throws HibernateException {
        return (Serializable) value;
    }

    @Override
    public boolean equals(Object x, Object y) throws HibernateException {
        return x == y;
    }

    @Override
    public int hashCode(Object x) throws HibernateException {
        return x.hashCode();
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public Object replace(Object original, Object target, Object owner) throws HibernateException {
        return original;
    }

    @Override
    public Object nullSafeGet(ResultSet rs, String[] names,
            SharedSessionContractImplementor session, Object owner)
            throws HibernateException, SQLException {

        Object identifier = type.get(rs, names[0], session);
        if (rs.wasNull()) {
            return null;
        }

        try {
            return valueOfMethod.invoke(enumClass, new Object[] { identifier });

        } catch (Exception e) {
            throw new HibernateException("Exception while invoking valueOf method '"
                    + valueOfMethod.getName() + "' of enumeration class '" + enumClass + "'", e);
        }

    }

    @Override
    public void nullSafeSet(PreparedStatement st, Object value, int index,
            SharedSessionContractImplementor session) throws HibernateException, SQLException {

        try {
            if (value == null) {
                st.setNull(index, ((AbstractSingleColumnStandardBasicType<?>) type).sqlType());

            } else {
                Object identifier = identifierMethod.invoke(value, new Object[0]);
                type.nullSafeSet(st, identifier, index, session);

            }
        } catch (Exception e) {
            throw new HibernateException("Exception while invoking identifierMethod '"
                    + identifierMethod.getName() + "' of enumeration class '" + enumClass + "'", e);
        }

    }

}
