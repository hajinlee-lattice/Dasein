/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.cascading.runtime.hashJoin;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class JoinPrepareMapper implements MapFunction<Tuple, Tuple2<Tuple, Tuple[]>> {

    /**
     *
     */
    private static final long serialVersionUID = -4204915813975728133L;
    final private Tuple2<Tuple, Tuple[]> out;
    private final int initialPos;
    final private Fields schema;
    final private Fields keyFields;
    final private Tuple empty = new Tuple();

    public JoinPrepareMapper(int numJoinInputs, Fields schema, Fields keyFields) {
        this(numJoinInputs, 0, schema, keyFields);
    }

    public JoinPrepareMapper(int numJoinInputs, int initialPos, Fields schema, Fields keyFields) {
        this.schema = schema;
        this.keyFields = keyFields;
        this.initialPos = initialPos;

        Tuple[] tupleList = new Tuple[numJoinInputs];
        for (int i = 0; i < numJoinInputs - 1; i++) {
            tupleList[i] = new Tuple();
        }
        out = new Tuple2<>(null, tupleList);
    }

    @Override
    public Tuple2<Tuple, Tuple[]> map(Tuple tuple) throws Exception {
        if (keyFields == null) {
            out.f0 = empty;
        } else {
            out.f0 = tuple.get(schema, keyFields);
        }
        out.f1[initialPos] = tuple;
        return out;
    }

}
