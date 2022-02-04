/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.failure;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.scheduler.exceptionhistory.FailureHandlingResultSnapshot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Set;
import java.util.function.Function;

/** Failure object. */
public abstract class Failure {
    private final Throwable cause;

    public Failure(Throwable cause) {
        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }

    public boolean originatesAt(ExecutionVertexID id) {
        return false;
    }

    public abstract Failure replaceCause(Throwable cause);

    public abstract FailureHandlingResultSnapshot createFailureHandlingResultSnapshot(
            Set<ExecutionVertexID> concurrentVertexIds,
            long timestamp,
            Function<ExecutionVertexID, Execution> latestExecutionLookup);
}
