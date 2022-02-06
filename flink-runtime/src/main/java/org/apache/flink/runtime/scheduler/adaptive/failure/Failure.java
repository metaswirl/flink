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

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Failure object. */
public abstract class Failure {
    private final Throwable cause;
    private final long timestamp;

    public Failure(Throwable cause) {
        this.cause = cause;
        this.timestamp = System.currentTimeMillis();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Throwable getCause() {
        return cause;
    }

    public abstract Failure replaceCause(Throwable cause);

    public abstract ExceptionHistoryEntry toExceptionHistoryEntry(
            Function<ExecutionVertexID, Optional<ExecutionVertex>> lookup);

    public abstract RootExceptionHistoryEntry toRootExceptionHistoryEntry(
            Function<ExecutionVertexID, Optional<ExecutionVertex>> lookup,
            Set<ExceptionHistoryEntry> concurrentEntries);

    public static Failure createLocal(Throwable cause) {
        return new LocalFailureWithoutExecutionVertexID(cause);
    }

    public static Failure createLocal(Throwable cause, ExecutionVertexID evid) {
        return new LocalFailure(cause, evid);
    }

    public static Failure createLocal(Throwable cause, Optional<ExecutionVertexID> evid) {
        return evid.map(id -> createLocal(cause, id)).orElse(createLocal(cause));
    }

    public static Failure createGlobal(Throwable cause) {
        return new GlobalFailure(cause);
    }
}
