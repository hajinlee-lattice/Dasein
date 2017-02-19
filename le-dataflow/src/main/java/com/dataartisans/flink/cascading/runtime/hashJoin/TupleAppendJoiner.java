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

import cascading.tuple.Tuple;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TupleAppendJoiner implements JoinFunction<Tuple2<Tuple, Tuple[]>, Tuple, Tuple2<Tuple, Tuple[]>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4322282585772670002L;
	private int tupleListPos;

	public TupleAppendJoiner(int tupleListPos) {
		this.tupleListPos = tupleListPos;
	}

	@Override
	public Tuple2<Tuple, Tuple[]> join(Tuple2<Tuple, Tuple[]> left, Tuple right) throws Exception {

		left.f1[tupleListPos] = right;
		return left;
	}
}
