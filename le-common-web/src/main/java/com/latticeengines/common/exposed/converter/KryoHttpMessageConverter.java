package com.latticeengines.common.exposed.converter;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyMapSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptySetSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonMapSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonSetSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.cglib.CGLibProxySerializer;
import de.javakaffee.kryoserializers.guava.ArrayListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;
import de.javakaffee.kryoserializers.guava.LinkedHashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.LinkedListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ReverseListSerializer;
import de.javakaffee.kryoserializers.guava.TreeMultimapSerializer;
import de.javakaffee.kryoserializers.guava.UnmodifiableNavigableSetSerializer;

public class KryoHttpMessageConverter extends AbstractHttpMessageConverter<Object> {

    private static final Logger log = LoggerFactory.getLogger(KryoHttpMessageConverter.class);
    public static final MediaType KRYO = new MediaType("application", "x-kryo");
    public static final String KRYO_VALUE = "application/x-kryo";

    private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {

        private int clzId = 1;

        @Override
        protected Kryo initialValue() {
            log.info("Initializing a Kryo");
            Kryo kryo = new Kryo();
            kryo.register( Arrays.asList( "" ).getClass(), new ArraysAsListSerializer() );
            kryo.register( Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer() );
            kryo.register( Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer() );
            kryo.register( Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer() );
            kryo.register( Collections.singletonList( "" ).getClass(), new CollectionsSingletonListSerializer() );
            kryo.register( Collections.singleton( "" ).getClass(), new CollectionsSingletonSetSerializer() );
            kryo.register( Collections.singletonMap( "", "" ).getClass(), new CollectionsSingletonMapSerializer() );
            kryo.register( GregorianCalendar.class, new GregorianCalendarSerializer() );
            kryo.register( InvocationHandler.class, new JdkProxySerializer() );
            UnmodifiableCollectionsSerializer.registerSerializers( kryo );
            SynchronizedCollectionsSerializer.registerSerializers( kryo );

            // custom serializers for non-jdk libs

            // register CGLibProxySerializer, works in combination with the appropriate action in handleUnregisteredClass (see below)
            kryo.register( CGLibProxySerializer.CGLibProxyMarker.class, new CGLibProxySerializer() );
            // guava ImmutableList, ImmutableSet, ImmutableMap, ImmutableMultimap, ReverseList, UnmodifiableNavigableSet
            ImmutableListSerializer.registerSerializers( kryo );
            ImmutableSetSerializer.registerSerializers( kryo );
            ImmutableMapSerializer.registerSerializers( kryo );
            ImmutableMultimapSerializer.registerSerializers( kryo );
            ReverseListSerializer.registerSerializers( kryo );
            UnmodifiableNavigableSetSerializer.registerSerializers( kryo );
            // guava ArrayListMultimap, HashMultimap, LinkedHashMultimap, LinkedListMultimap, TreeMultimap
            ArrayListMultimapSerializer.registerSerializers( kryo );
            HashMultimapSerializer.registerSerializers( kryo );
            LinkedHashMultimapSerializer.registerSerializers( kryo );
            LinkedListMultimapSerializer.registerSerializers( kryo );
            TreeMultimapSerializer.registerSerializers( kryo );
            return kryo;
        }

        private void register(Kryo kryo, Class<?> clz) {
            kryo.register(clz, clzId++);
        }
    };

    public KryoHttpMessageConverter() {
        super(KRYO);
    }

    @Override
    protected boolean supports(Class<?> clazz) {
        return Object.class.isAssignableFrom(clazz);
    }

    @Override
    protected Object readInternal(Class<? extends Object> clazz, HttpInputMessage inputMessage) throws IOException {
        Input input = new Input(inputMessage.getBody());
        return kryoThreadLocal.get().readClassAndObject(input);
    }

    @Override
    protected void writeInternal(Object object, HttpOutputMessage outputMessage) throws IOException {
        Output output = new Output(outputMessage.getBody());
        kryoThreadLocal.get().writeClassAndObject(output, object);
        output.flush();
    }

    @Override
    protected MediaType getDefaultContentType(Object object) {
        return KRYO;
    }
}