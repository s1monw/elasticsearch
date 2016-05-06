/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;

public class QuotactlTests extends ESTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue("quota only works on linux", Constants.LINUX);
    }

    public void testLibraryLoaded() {
        assertTrue("failed to load quotactl library", Quotactl.isLoaded());
    }

    public void testFoo() {
        Path tempDir = createTempDir();
        assertEquals(-1, Quotactl.getAvaliableSpace(tempDir, 1000, 1024));
    }
}
