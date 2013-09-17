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

package org.elasticsearch;

import org.elasticsearch.action.*;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.mlt.MoreLikeThisRequestBuilder;
import org.elasticsearch.action.percolate.*;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.termvector.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;

import java.util.Random;

public class RandomizedClient implements Client {
    private final Client delegate;
    private final Random random;
    private final SearchType randomSearchType;
    
    public RandomizedClient(Client delegate, Random random) {
        this.delegate = delegate;
        this.random = random;
        switch (random.nextInt(5)) {
        case 3:
        case 2:
            randomSearchType = SearchType.DFS_QUERY_THEN_FETCH;
            break;
        default:
            randomSearchType = SearchType.QUERY_THEN_FETCH;
            break;
        }
    }

    public void close() {
        delegate.close();
    }

    public AdminClient admin() {
        return delegate.admin();
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(
            Action<Request, Response, RequestBuilder> action, Request request) {
        return delegate.execute(action, request);
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(
            Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        delegate.execute(action, request, listener);
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(
            Action<Request, Response, RequestBuilder> action) {
        return delegate.prepareExecute(action);
    }

    public ActionFuture<IndexResponse> index(IndexRequest request) {
        return delegate.index(request);
    }

    public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        delegate.index(request, listener);
    }

    public IndexRequestBuilder prepareIndex() {
        return delegate.prepareIndex();
    }

    public ActionFuture<UpdateResponse> update(UpdateRequest request) {
        return delegate.update(request);
    }

    public void update(UpdateRequest request, ActionListener<UpdateResponse> listener) {
        delegate.update(request, listener);
    }

    public UpdateRequestBuilder prepareUpdate() {
        return delegate.prepareUpdate();
    }

    public UpdateRequestBuilder prepareUpdate(String index, String type, String id) {
        return delegate.prepareUpdate(index, type, id);
    }

    public IndexRequestBuilder prepareIndex(String index, String type) {
        return delegate.prepareIndex(index, type);
    }

    public IndexRequestBuilder prepareIndex(String index, String type, String id) {
        return delegate.prepareIndex(index, type, id);
    }

    public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return delegate.delete(request);
    }

    public void delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        delegate.delete(request, listener);
    }

    public DeleteRequestBuilder prepareDelete() {
        return delegate.prepareDelete();
    }

    public DeleteRequestBuilder prepareDelete(String index, String type, String id) {
        return delegate.prepareDelete(index, type, id);
    }

    public ActionFuture<BulkResponse> bulk(BulkRequest request) {
        return delegate.bulk(request);
    }

    public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        delegate.bulk(request, listener);
    }

    public BulkRequestBuilder prepareBulk() {
        return delegate.prepareBulk();
    }

    public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request) {
        return delegate.deleteByQuery(request);
    }

    public void deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        delegate.deleteByQuery(request, listener);
    }

    public DeleteByQueryRequestBuilder prepareDeleteByQuery(String... indices) {
        return delegate.prepareDeleteByQuery(indices);
    }

    public ActionFuture<GetResponse> get(GetRequest request) {
        return delegate.get(request);
    }

    public void get(GetRequest request, ActionListener<GetResponse> listener) {
        delegate.get(request, listener);
    }

    public GetRequestBuilder prepareGet() {
        return delegate.prepareGet();
    }

    public GetRequestBuilder prepareGet(String index, String type, String id) {
        return delegate.prepareGet(index, type, id);
    }

    public ActionFuture<MultiGetResponse> multiGet(MultiGetRequest request) {
        return delegate.multiGet(request);
    }

    public void multiGet(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        delegate.multiGet(request, listener);
    }

    public MultiGetRequestBuilder prepareMultiGet() {
        return delegate.prepareMultiGet();
    }

    public ActionFuture<CountResponse> count(CountRequest request) {
        return delegate.count(request);
    }

    public void count(CountRequest request, ActionListener<CountResponse> listener) {
        delegate.count(request, listener);
    }

    public CountRequestBuilder prepareCount(String... indices) {
        return delegate.prepareCount(indices);
    }

    public ActionFuture<SuggestResponse> suggest(SuggestRequest request) {
        return delegate.suggest(request);
    }

    public void suggest(SuggestRequest request, ActionListener<SuggestResponse> listener) {
        delegate.suggest(request, listener);
    }

    public SuggestRequestBuilder prepareSuggest(String... indices) {
        return delegate.prepareSuggest(indices);
    }

    public ActionFuture<SearchResponse> search(SearchRequest request) {
        return delegate.search(request);
    }

    public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        delegate.search(request, listener);
    }

    public SearchRequestBuilder prepareSearch(String... indices) {
        SearchRequestBuilder prepareSearch = delegate.prepareSearch(indices);
        prepareSearch.setSearchType(randomSearchType);
        return prepareSearch;
    }

    public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        return delegate.searchScroll(request);
    }

    public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        delegate.searchScroll(request, listener);
    }

    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return delegate.prepareSearchScroll(scrollId);
    }

    public ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request) {
        return delegate.multiSearch(request);
    }

    public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        delegate.multiSearch(request, listener);
    }

    public MultiSearchRequestBuilder prepareMultiSearch() {
        return delegate.prepareMultiSearch();
    }

    public ActionFuture<SearchResponse> moreLikeThis(MoreLikeThisRequest request) {
        return delegate.moreLikeThis(request);
    }

    public void moreLikeThis(MoreLikeThisRequest request, ActionListener<SearchResponse> listener) {
        delegate.moreLikeThis(request, listener);
    }

    public MoreLikeThisRequestBuilder prepareMoreLikeThis(String index, String type, String id) {
        return delegate.prepareMoreLikeThis(index, type, id);
    }

    public ActionFuture<TermVectorResponse> termVector(TermVectorRequest request) {
        return delegate.termVector(request);
    }

    public void termVector(TermVectorRequest request, ActionListener<TermVectorResponse> listener) {
        delegate.termVector(request, listener);
    }

    public TermVectorRequestBuilder prepareTermVector(String index, String type, String id) {
        return delegate.prepareTermVector(index, type, id);
    }

    public ActionFuture<MultiTermVectorsResponse> multiTermVectors(MultiTermVectorsRequest request) {
        return delegate.multiTermVectors(request);
    }

    public void multiTermVectors(MultiTermVectorsRequest request, ActionListener<MultiTermVectorsResponse> listener) {
        delegate.multiTermVectors(request, listener);
    }

    public MultiTermVectorsRequestBuilder prepareMultiTermVectors() {
        return delegate.prepareMultiTermVectors();
    }

    public ActionFuture<PercolateResponse> percolate(PercolateRequest request) {
        return delegate.percolate(request);
    }

    public void percolate(PercolateRequest request, ActionListener<PercolateResponse> listener) {
        delegate.percolate(request, listener);
    }

    public PercolateRequestBuilder preparePercolate() {
        return delegate.preparePercolate();
    }

    public ActionFuture<MultiPercolateResponse> multiPercolate(MultiPercolateRequest request) {
        return delegate.multiPercolate(request);
    }

    public void multiPercolate(MultiPercolateRequest request, ActionListener<MultiPercolateResponse> listener) {
        delegate.multiPercolate(request, listener);
    }

    public MultiPercolateRequestBuilder prepareMultiPercolate() {
        return delegate.prepareMultiPercolate();
    }

    public ExplainRequestBuilder prepareExplain(String index, String type, String id) {
        return delegate.prepareExplain(index, type, id);
    }

    public ActionFuture<ExplainResponse> explain(ExplainRequest request) {
        return delegate.explain(request);
    }

    public void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        delegate.explain(request, listener);
    }

    public ClearScrollRequestBuilder prepareClearScroll() {
        return delegate.prepareClearScroll();
    }

    public ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest request) {
        return delegate.clearScroll(request);
    }

    public void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener) {
        delegate.clearScroll(request, listener);
    }
    
}
