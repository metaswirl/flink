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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.adaptive.failure.Failure;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Duration;

/**
 * The {@link FailureResult} describes how a failure shall be handled. Currently, there are two
 * alternatives: Either restarting the job or failing it.
 */
final class FailureResult {
    @Nullable private final Duration backoffTime;

    /**
     * The {@link ExecutionVertexID} refering to the {@link ExecutionVertex} the failure is
     * originating from or {@code null} if it's a global failure.
     */
    private Failure failure;

    private FailureResult(Failure failure, @Nullable Duration backoffTime) {
        this.failure = failure;
        this.backoffTime = backoffTime;
    }

    boolean canRestart() {
        return backoffTime != null;
    }

    Duration getBackoffTime() {
        Preconditions.checkState(
                canRestart(), "Failure result must be restartable to return a backoff time.");
        return backoffTime;
    }

    Throwable getFailureCause() {
        return failure.getCause();
    }

    /**
     * Creates a FailureResult which allows to restart the job.
     *
     * @param failure failure describes reason for restarting the job
     * @param backoffTime backoffTime to wait before restarting the job
     * @return FailureResult which allows to restart the job
     */
    static FailureResult canRestart(Failure failure, Duration backoffTime) {
        return new FailureResult(failure, backoffTime);
    }

    /**
     * Creates FailureResult which does not allow to restart the job.
     *
     * @param failure failure describes reason for not restarting the job
     * @return FailureResult which does not allow to restart the job
     */
    static FailureResult canNotRestart(Failure failure) {
        return new FailureResult(failure, null);
    }
}
