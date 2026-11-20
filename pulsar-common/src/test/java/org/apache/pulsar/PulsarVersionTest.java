/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import org.testng.annotations.Test;

/**
 * Verifies the values that {@link PulsarVersion} reports.
 *
 * <p>Reproducible builds (issue #24718) require the jar to embed only values derivable from the
 * sources, so the build no longer captures the builder-identifying metadata: the git branch, the
 * build user, the build host and the build time. Those getters must report {@code "unknown"}
 * regardless of how the resource was generated, and the version and git sha getters must keep
 * working.
 */
public class PulsarVersionTest {

    @Test
    public void versionIsReported() {
        String version = PulsarVersion.getVersion();
        assertNotEquals(version, PulsarVersion.UNKNOWN, "the generated resource must carry the version");
    }

    @Test
    public void builderIdentityIsNeverEmbedded() {
        // Only source-derivable values may be embedded in pulsar-version.properties; the values
        // that identify the machine, the user and the clock that produced the build are not, so
        // every rebuild from the same sources produces the identical jar.
        assertEquals(PulsarVersion.getGitBranch(), PulsarVersion.UNKNOWN);
        assertEquals(PulsarVersion.getBuildUser(), PulsarVersion.UNKNOWN);
        assertEquals(PulsarVersion.getBuildHost(), PulsarVersion.UNKNOWN);
        assertEquals(PulsarVersion.getBuildTime(), PulsarVersion.UNKNOWN);
    }
}
