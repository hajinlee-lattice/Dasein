package com.latticeengines.sparkdb.service.impl

import org.apache.avro.generic.GenericData.Record
import org.apache.spark.serializer.KryoRegistrator

import com.esotericsoftware.kryo.Kryo

class LedpKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    //kryo.register(classOf[Record], AvroSerializer.asAvroSerializer)
  }
}