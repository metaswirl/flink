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
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Represents a local failure, i.e., a failure limited to a part of the ExecutionGraph. */
public class LocalFailure extends Failure {
    private final ExecutionVertexID id;

    public LocalFailure(Throwable cause, ExecutionVertexID id) {
        super(cause);
        this.id = id;
    }

    @Override
    public Failure replaceCause(Throwable cause) {
        return new LocalFailure(cause, id);
    }

    private ExceptionHistoryEntry createExceptionHistoryEntry(Execution execution) {
        return ExceptionHistoryEntry.create(execution, execution.getVertexWithAttempt());
    }

    @Override
    public ExceptionHistoryEntry toExceptionHistoryEntry(
            Function<ExecutionVertexID, Optional<ExecutionVertex>> lookup) {
        return lookup.apply(id)
                .map(ExecutionVertex::getCurrentExecutionAttempt)
                .map(this::createExceptionHistoryEntry)
                .orElse(new ExceptionHistoryEntry(getCause(), getTimestamp(), null, null));
    }

    @Override
    public RootExceptionHistoryEntry toRootExceptionHistoryEntry(
            Function<ExecutionVertexID, Optional<ExecutionVertex>> lookup,
            Set<ExceptionHistoryEntry> concurrentEntries) {

        return lookup.apply(id)
                .map(ExecutionVertex::getCurrentExecutionAttempt)
                .map(
                        execution ->
                                new RootExceptionHistoryEntry(
                                        getCause(),
                                        getTimestamp(),
                                        execution.getVertexWithAttempt(),
                                        execution.getAssignedResourceLocation(),
                                        concurrentEntries))
                .orElse(
                        new RootExceptionHistoryEntry(
                                getCause(), getTimestamp(), null, null, concurrentEntries));
    }
}
