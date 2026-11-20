/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.gradle.api.java.archives.Attributes
import org.gradle.api.tasks.bundling.Jar
import org.gradle.testfixtures.ProjectBuilder
import org.testng.Assert.assertEquals
import org.testng.Assert.assertFalse
import org.testng.annotations.Test
import java.io.ByteArrayInputStream
import java.util.jar.JarInputStream

class ReproducibleManifestTest {

    /** A fresh Jar task with the decorator installed and a plugin-style stamping action registered. */
    private fun jarWith(hidden: Set<String>, stamped: Map<String, Any>): Jar {
        val project = ProjectBuilder.builder().build()
        val jar = project.tasks.register("jar", Jar::class.java).get()
        val file = project.rootDir.resolve("reproducible-manifest-test.jar")
        jar.destinationDirectory.set(file.parentFile)
        jar.archiveFileName.set(file.name)
        jar.manifest.attributes(mapOf("Nar-Id" to "test-nar", "Keep-Me" to "yes"))
        // The NAR plugin registers its stamping as a doFirst action, which runs after every action
        // registered later: the filter must survive it. Execute the action eagerly here, which the
        // test uses in place of a real task execution.
        jar.doFirst { jar.manifest.attributes(stamped) }
        jar.manifest = ReproducibleManifest(jar.manifest, hidden)
        return jar
    }

    @Test
    fun keepsUnrelatedAttributesAndDropsHiddenOnesStampedLater() {
        val jar = jarWith(
            hidden = setOf("Build-Timestamp", "Build-Tag"),
            stamped = mapOf("Build-Timestamp" to "2026-11-20T07:00:00Z", "Build-Tag" to "v-test-1-gabc"),
        )
        // Run the plugin-style stamping action, as task execution would.
        jar.actions.forEach { it.execute(jar) }
        val attributes = jar.manifest.effectiveManifest.attributes
        assertEquals(attributes["Nar-Id"], "test-nar")
        assertEquals(attributes["Keep-Me"], "yes")
        assertFalse(attributes.containsKey("Build-Timestamp"))
        assertFalse(attributes.containsKey("Build-Tag"))
    }

    @Test
    fun keepsEverythingWhenNothingIsHidden() {
        val jar = jarWith(
            hidden = emptySet(),
            stamped = mapOf("Build-Timestamp" to "2026-11-20T07:00:00Z"),
        )
        jar.actions.forEach { it.execute(jar) }
        val attributes = jar.manifest.effectiveManifest.attributes
        assertEquals(attributes["Build-Timestamp"], "2026-11-20T07:00:00Z")
        assertEquals(attributes["Nar-Id"], "test-nar")
        assertEquals(attributes["Keep-Me"], "yes")
    }

    @Test
    fun configurationTimeAttributesStillWorkThroughTheDecorator() {
        val jar = jarWith(hidden = setOf("Build-Timestamp"), stamped = emptyMap())
        // Adding attributes through the decorator (as plugins applied later would) must work.
        jar.manifest.attributes(mapOf("Extra" to "value", "Build-Timestamp" to "hidden-anyway"))
        val attributes: Attributes = jar.manifest.effectiveManifest.attributes
        assertEquals(attributes["Nar-Id"], "test-nar")
        assertEquals(attributes["Extra"], "value")
        assertFalse(attributes.containsKey("Build-Timestamp"))
    }

    @Test
    fun jarTaskWritesTheFilteredManifest() {
        val jar = jarWith(
            hidden = setOf("Build-Timestamp", "Build-Tag"),
            stamped = mapOf("Build-Timestamp" to "2026-11-20T07:00:00Z"),
        )
        val file = jar.archiveFile.get().asFile
        try {
            jar.actions.forEach { it.execute(jar) }
            JarInputStream(ByteArrayInputStream(file.readBytes())).use { input ->
                val attributes = input.manifest.mainAttributes
                assertEquals(attributes.getValue("Nar-Id"), "test-nar")
                assertEquals(attributes.getValue("Keep-Me"), "yes")
                assertFalse(attributes.containsKey(java.util.jar.Attributes.Name("Build-Timestamp")))
                assertFalse(attributes.containsKey(java.util.jar.Attributes.Name("Build-Tag")))
            }
        } finally {
            file.delete()
        }
    }
}
