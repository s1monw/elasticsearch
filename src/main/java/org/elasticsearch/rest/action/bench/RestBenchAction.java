/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.rest.action.bench;

import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.bench.BenchRequest;
import org.elasticsearch.action.bench.BenchRequestBuilder;
import org.elasticsearch.action.bench.BenchResponse;
import org.elasticsearch.action.bench.BenchSpecBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.admin.indices.cache.clear.RestClearIndicesCacheAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

/**
 *
 */
public class RestBenchAction extends BaseRestHandler {

    @Inject
    public RestBenchAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_bench", this);
        controller.registerHandler(POST, "/_bench", this);
        controller.registerHandler(GET, "/{index}/_bench", this);
        controller.registerHandler(POST, "/{index}/_bench", this);
        controller.registerHandler(GET, "/{index}/{type}/_bench", this);
        controller.registerHandler(POST, "/{index}/{type}/_bench", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        ClearIndicesCacheRequest clearIndicesCacheRequest = null;
        if (request.paramAsBoolean("clear_caches", false)) {
            clearIndicesCacheRequest = new ClearIndicesCacheRequest(indices);
            RestClearIndicesCacheAction.fromRequest(request, clearIndicesCacheRequest);
        }

        BenchRequest benchRequest;
        try {
            BenchRequestBuilder builder = new BenchRequestBuilder(client, indices);
            builder.setClearCaches(clearIndicesCacheRequest);
            benchRequest = parse(builder, request.content(), request.contentUnsafe(), indices, types);
            benchRequest.listenerThreaded(false);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("failed to parse search request parameters", e);
            }
            try {
                XContentBuilder builder = restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }
        client.bench(benchRequest, new ActionListener<BenchResponse>() {
            @Override
            public void onResponse(BenchResponse response) {
                try {
                    XContentBuilder builder = restContentBuilder(request);
                    builder.startObject();
                    response.toXContent(builder, request);
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
                } catch (Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("failed to execute bench (building response)", e);
                    }
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    public static BenchRequest parse(BenchRequestBuilder builder, BytesReference data, boolean contentUnsafe, String[] indices, String[] types) throws Exception {
        XContent xContent = XContentFactory.xContent(data);
        XContentParser p = xContent.createParser(data);
        XContentParser.Token token = p.nextToken();
        assert token == XContentParser.Token.START_OBJECT;
        String fieldName = null;
        while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
            switch (token) {
                case START_ARRAY:
                    if ("requests".equals(fieldName)) {
                        while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
                            assert token == XContentParser.Token.START_OBJECT;
                            XContentBuilder payloadBuilder = XContentFactory.contentBuilder(p.contentType()).copyCurrentStructure(p);
                            SearchRequest req = new SearchRequest(indices);
                            req.types(types);
                            req.source(payloadBuilder.bytes(), contentUnsafe);
                            builder.addSearchRequest(req);
                        }
                    } else if ("competitors".equals(fieldName)) {
                        while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
                            builder.addCompetitor(parse(p, contentUnsafe, indices, types));
                        }
                    } else {
                        throw new ElasticSearchParseException("Failed parsing array field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case START_OBJECT:
                    throw new ElasticSearchParseException("Failed parsing object field [" + fieldName + "] field is not recognized");
                case FIELD_NAME:
                    fieldName = p.text();
                    break;
                case VALUE_NUMBER:
                    if ("num_executors".equals(fieldName) || "numExecutors".equals(fieldName)) {
                        builder.setNumExecutors(p.intValue());
                    } else {
                        throw new ElasticSearchParseException("Failed parsing numeric field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case VALUE_BOOLEAN:
                    throw new ElasticSearchParseException("Failed parsing boolean field [" + fieldName + "] field is not recognized");
                case VALUE_STRING:
                    if ("id".equals(fieldName)) {
                        builder.setBenchmarkId(p.text());
                    } else {
                        throw new ElasticSearchParseException("Failed parsing string field [" + fieldName + "] field is not recognized");
                    }
                    break;
                default:
                    throw new ElasticSearchParseException("Failed parsing " + token.name() + " field [" + fieldName + "] field is not recognized");
            }
        }

        return builder.request();
    }

    public static BenchSpecBuilder parse(XContentParser p, boolean contentUnsafe, String[] indices, String[] types) throws Exception {
        XContentParser.Token token = p.currentToken();
        BenchSpecBuilder builder = new BenchSpecBuilder();
        assert token == XContentParser.Token.START_OBJECT;
        String fieldName = null;
        while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
            switch (token) {
                case START_ARRAY:
                    if ("requests".equals(fieldName)) {
                        while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
                            assert token == XContentParser.Token.START_OBJECT;
                            XContentBuilder payloadBuilder = XContentFactory.contentBuilder(p.contentType()).copyCurrentStructure(p);
                            SearchRequest req = new SearchRequest(indices);
                            req.types(types);
                            req.source(payloadBuilder.bytes(), contentUnsafe);
                            builder.addSearchRequest(req);
                        }
                    } else if ("indices".equals(fieldName)) {
                        List<String> perCompetitorIndices = new ArrayList<String>();
                        while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                perCompetitorIndices.add(p.text());
                            } else {
                                throw new ElasticSearchParseException("Failed parsing array field [" + fieldName + "] expected string values but got: "  + token);
                            }
                        }
                        builder.setIndices(perCompetitorIndices.toArray(new String[perCompetitorIndices.size()]));
                    } else {
                        throw new ElasticSearchParseException("Failed parsing array field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case START_OBJECT:
                    if ("defaults".equals(fieldName)) {
                        assert token == XContentParser.Token.START_OBJECT;
                        XContentBuilder payloadBuilder = XContentFactory.contentBuilder(p.contentType()).copyCurrentStructure(p);
                        SearchRequest req = new SearchRequest(indices);
                        req.source(payloadBuilder.bytes(), contentUnsafe);
                        assert req.indices() != null;
                        builder.defaultSearchReqeust(req);
                    } else {
                        throw new ElasticSearchParseException("Failed parsing object field [" + fieldName + "] field is not recognized");
                    }
                case FIELD_NAME:
                    fieldName = p.text();
                    break;
                case VALUE_NUMBER:
                    if ("multiplier".equals(fieldName)) {
                        builder.setMultiplier(p.intValue());
                    } else if ("num_slowest".equals(fieldName) || "numSlowest".equals(fieldName)) {
                        builder.setNumSlowest(p.intValue());
                    } else if ("iterations".equals(fieldName)) {
                        builder.setIterations(p.intValue());
                    } else if ("concurrency".equals(fieldName)) {
                        builder.setConcurrency(p.intValue());
                    } else {
                        throw new ElasticSearchParseException("Failed parsing numeric field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case VALUE_BOOLEAN:
                    if ("warmup".equals(fieldName)) {
                        builder.setWarmup(p.booleanValue());
                    } else {
                        throw new ElasticSearchParseException("Failed parsing boolean field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case VALUE_STRING:
                    if ("name".equals(fieldName)) {
                        builder.setName(p.text());
                    } else if ("search_type".equals(fieldName) || "searchType".equals(fieldName)) {
                        builder.setSearchType(SearchType.fromString(p.text()));
                    } else {
                        throw new ElasticSearchParseException("Failed parsing string field [" + fieldName + "] field is not recognized");
                    }
                    break;
                default:
                    throw new ElasticSearchParseException("Failed parsing " + token.name() + " field [" + fieldName + "] field is not recognized");
            }
        }
        return builder;
    }

}
