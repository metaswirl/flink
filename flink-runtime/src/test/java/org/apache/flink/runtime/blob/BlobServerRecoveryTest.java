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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKeyTest.verifyKeyDifferentHashEquals;
import static org.apache.flink.runtime.blob.BlobServerGetTest.verifyDeleted;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the recovery of files of a {@link BlobServer} from a HA store. */
public class BlobServerRecoveryTest extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    /**
     * Tests that with {@link HighAvailabilityMode#ZOOKEEPER} distributed JARs are recoverable from
     * any participating BlobServer.
     */
    @Test
    public void testBlobServerRecovery() throws Exception {
        Configuration config = new Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, TEMPORARY_FOLDER.newFolder().getPath());

        BlobStoreService blobStoreService = null;

        try {
            blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

            testBlobServerRecovery(config, blobStoreService, TEMPORARY_FOLDER.newFolder());
        } finally {
            if (blobStoreService != null) {
                blobStoreService.closeAndCleanupAllData();
            }
        }
    }

    /**
     * Helper to test that the {@link BlobServer} recovery from its HA store works.
     *
     * <p>Uploads two BLOBs to one {@link BlobServer} and expects a second one to be able to
     * retrieve them via a shared HA store upon request of a {@link BlobCacheService}.
     *
     * @param config blob server configuration (including HA settings like {@link
     *     HighAvailabilityOptions#HA_STORAGE_PATH} and {@link
     *     HighAvailabilityOptions#HA_CLUSTER_ID}) used to set up <tt>blobStore</tt>
     * @param blobStore shared HA blob store to use
     * @throws IOException in case of failures
     */
    public static void testBlobServerRecovery(
            final Configuration config, final BlobStore blobStore, final File blobStorage)
            throws IOException {
        final String clusterId = config.getString(HighAvailabilityOptions.HA_CLUSTER_ID);
        String storagePath =
                config.getString(HighAvailabilityOptions.HA_STORAGE_PATH) + "/" + clusterId;
        Random rand = new Random();

        try (BlobServer server0 =
                        new BlobServer(config, new File(blobStorage, "server0"), blobStore);
                BlobServer server1 =
                        new BlobServer(config, new File(blobStorage, "server1"), blobStore);
                // use VoidBlobStore as the HA store to force download from server[1]'s HA store
                BlobCacheService cache1 =
                        new BlobCacheService(
                                config,
                                new File(blobStorage, "cache1"),
                                new VoidBlobStore(),
                                new InetSocketAddress("localhost", server1.getPort()))) {

            server0.start();
            server1.start();

            // Random data
            byte[] expected = new byte[1024];
            rand.nextBytes(expected);
            byte[] expected2 = Arrays.copyOfRange(expected, 32, 288);

            BlobKey[] keys = new BlobKey[2];
            BlobKey nonHAKey;

            // Put job-related HA data
            JobID[] jobId = new JobID[] {new JobID(), new JobID()};
            keys[0] = put(server0, jobId[0], expected, PERMANENT_BLOB); // Request 1
            keys[1] = put(server0, jobId[1], expected2, PERMANENT_BLOB); // Request 2

            // put non-HA data
            nonHAKey = put(server0, jobId[0], expected2, TRANSIENT_BLOB);
            verifyKeyDifferentHashEquals(keys[1], nonHAKey);

            // check that the storage directory exists
            final Path blobServerPath = new Path(storagePath, "blob");
            FileSystem fs = blobServerPath.getFileSystem();
            assertTrue("Unknown storage dir: " + blobServerPath, fs.exists(blobServerPath));

            // Verify HA requests from cache1 (connected to server1) with no immediate access to the
            // file
            verifyContents(cache1, jobId[0], keys[0], expected);
            verifyContents(cache1, jobId[1], keys[1], expected2);

            // Verify non-HA file is not accessible from server1
            verifyDeleted(cache1, jobId[0], nonHAKey);

            // Remove again
            server1.cleanupJob(jobId[0], true);
            server1.cleanupJob(jobId[1], true);

            // Verify everything is clean
            assertTrue("HA storage directory does not exist", fs.exists(new Path(storagePath)));
            if (fs.exists(blobServerPath)) {
                final org.apache.flink.core.fs.FileStatus[] recoveryFiles =
                        fs.listStatus(blobServerPath);
                ArrayList<String> filenames = new ArrayList<>(recoveryFiles.length);
                for (org.apache.flink.core.fs.FileStatus file : recoveryFiles) {
                    filenames.add(file.toString());
                }
                fail("Unclean state backend: " + filenames);
            }
        }
    }
}
