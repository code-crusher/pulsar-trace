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
import org.gradle.api.java.archives.Manifest

/**
 * A [Manifest] decorator that keeps specified attributes out of the rendered (effective) manifest.
 *
 * The Pulsar NAR Gradle plugin (io.github.merlimat.nar, see the `nar` entry in the version
 * catalog) stamps `Build-Timestamp` (the wall-clock time of the build) and, when no explicit
 * `buildTag` is configured, `Build-Tag` (`git describe` output) into the NAR manifest from a
 * `doFirst` action at execution time. Neither attribute can be derived from the sources, so no
 * independent rebuild can ever reproduce them, which makes the published NARs fail the
 * reproducible-builds comparison of issue #24718. Nothing in the Pulsar runtime reads them —
 * the `NarClassLoader` only looks at `Nar-Id` — so they are dropped here instead.
 *
 * Filtering has to happen on the *effective* manifest, at render time: the plugin's `doFirst`
 * action runs after every action registered later (Gradle runs `doFirst` actions in
 * last-registered-first order), so removing the attributes eagerly would be undone by the
 * plugin's own action. Gradle's `Manifest` interface has no removal API, so attributes the
 * plugin adds to the wrapped manifest are simply not carried over by [attributes].
 *
 * The decorator delegates everything else, including configuration-time `attributes`
 * mutation (which the plugin and the convention plugins use), so behavior is unchanged except
 * that the hidden keys vanish from the written `META-INF/MANIFEST.MF`.
 *
 * @param delegate the manifest to wrap, normally the task's current manifest
 * @param hiddenKeys the manifest attribute names to leave out of the rendered manifest
 */
class ReproducibleManifest(
    private val delegate: Manifest,
    private val hiddenKeys: Set<String>,
) : Manifest by delegate {

    override fun getEffectiveManifest(): Manifest {
        val effective = delegate.effectiveManifest
        return object : Manifest by effective {
            override fun getAttributes(): Attributes {
                val kept = LinkedHashMap<String, Any>()
                for ((key, value) in effective.attributes) {
                    if (key !in hiddenKeys) {
                        kept[key] = value
                    }
                }
                return FilteredAttributes(kept)
            }
        }
    }

    /** An [Attributes] view over a copied map, hiding nothing beyond what the copy already omits. */
    private class FilteredAttributes(values: Map<String, Any>) :
        Attributes, MutableMap<String, Any> by LinkedHashMap(values)
}
