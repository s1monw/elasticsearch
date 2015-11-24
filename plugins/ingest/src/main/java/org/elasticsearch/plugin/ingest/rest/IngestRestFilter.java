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

package org.elasticsearch.plugin.ingest.rest;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.*;

import static org.elasticsearch.plugin.ingest.IngestPlugin.*;
import static org.elasticsearch.plugin.ingest.IngestPlugin.PIPELINE_ID_PARAM_CONTEXT_KEY;

public class IngestRestFilter extends RestFilter {

    /* simonw: I think we should in core factor out the filter registration to simplify this and get away from guice - I can take care of this */
    @Inject
    public IngestRestFilter(RestController controller) {
        controller.registerFilter(this);
    }

    @Override
    public void process(RestRequest request, RestChannel channel, RestFilterChain filterChain) throws Exception {
        if (request.hasParam(PIPELINE_ID_PARAM)) {
            request.putInContext(PIPELINE_ID_PARAM_CONTEXT_KEY, request.param(PIPELINE_ID_PARAM));
        }
        filterChain.continueProcessing(request, channel);
    }
}
