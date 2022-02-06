/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.adaptive.failure.Failure;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import java.util.List;
import java.util.Optional;

/** State which describes a failing job which is currently being canceled. */
class Failing extends StateWithExecutionGraph {
    private final Context context;
    private final List<Failure> failureCollection;

    Failing(
            Context context,
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Logger logger,
            Throwable failureCause,
            ClassLoader userCodeClassLoader,
            List<Failure> failureCollection) {
        super(
                context,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                logger,
                userCodeClassLoader);
        this.context = context;
        this.failureCollection = failureCollection;

        getExecutionGraph().failJob(failureCause, System.currentTimeMillis());
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.FAILING;
    }

    @Override
    public void cancel() {
        context.goToCanceling(
                getExecutionGraph(), getExecutionGraphHandler(), getOperatorCoordinatorHandler());
    }

    private void handleFailure(Failure failure) {
        failureCollection.add(failure);
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        handleFailure(Failure.createGlobal(cause));
    }

    @Override
    boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionStateTransition) {
        final boolean successfulUpdate =
                getExecutionGraph().updateState(taskExecutionStateTransition);

        if (successfulUpdate
                && taskExecutionStateTransition.getExecutionState() == ExecutionState.FAILED) {
            handleFailure(
                    Failure.createLocal(
                            extractError(taskExecutionStateTransition),
                            extractExecutionVertexID(taskExecutionStateTransition)));
        }
        return successfulUpdate;
    }

    @Override
    void onGloballyTerminalState(JobStatus globallyTerminalState) {
        Optional<RootExceptionHistoryEntry> entry =
                convertFailures(getExecutionGraph()::getExecutionVertex, failureCollection);
        entry.ifPresent(context::archiveFailure);
        Preconditions.checkState(globallyTerminalState == JobStatus.FAILED);
        context.goToFinished(ArchivedExecutionGraph.createFrom(getExecutionGraph()));
    }

    /** Context of the {@link Failing} state. */
    interface Context extends StateWithExecutionGraph.Context {
        /**
         * Archive collection of failures.
         *
         * @param failure Collection of failures
         */
        void archiveFailure(RootExceptionHistoryEntry failure);

        /**
         * Transitions into the {@link Canceling} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Canceling} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Canceling} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Canceling} state
         */
        void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler);
    }

    static class Factory implements StateFactory<Failing> {

        private final Context context;
        private final Logger log;
        private final ExecutionGraph executionGraph;
        private final ExecutionGraphHandler executionGraphHandler;
        private final OperatorCoordinatorHandler operatorCoordinatorHandler;
        private final Throwable failureCause;
        private final ClassLoader userCodeClassLoader;
        private final List<Failure> failureCollection;

        public Factory(
                Context context,
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Logger log,
                Throwable failureCause,
                ClassLoader userCodeClassLoader,
                List<Failure> failureCollection) {
            this.context = context;
            this.log = log;
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandler = operatorCoordinatorHandler;
            this.failureCause = failureCause;
            this.userCodeClassLoader = userCodeClassLoader;
            this.failureCollection = failureCollection;
        }

        public Class<Failing> getStateClass() {
            return Failing.class;
        }

        public Failing getState() {
            return new Failing(
                    context,
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    log,
                    failureCause,
                    userCodeClassLoader,
                    failureCollection);
        }
    }
}
