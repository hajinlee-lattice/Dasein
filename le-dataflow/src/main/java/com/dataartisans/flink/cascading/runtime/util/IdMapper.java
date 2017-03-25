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

package com.dataartisans.flink.cascading.runtime.util;

import org.apache.flink.api.common.functions.MapFunction;

import cascading.tuple.Tuple;

public class IdMapper implements MapFunction<Tuple, Tuple> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7257538252404874006L;

	@Override
	public Tuple map(Tuple value) throws Exception {
		return value;
	}
}
