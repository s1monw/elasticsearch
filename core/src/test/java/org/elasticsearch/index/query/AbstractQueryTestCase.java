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

package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import com.fasterxml.jackson.core.io.JsonStringEncoder;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParserMapper;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public abstract class AbstractQueryTestCase<QB extends AbstractQueryBuilder<QB>> extends ESTestCase {

    private static final GeohashGenerator geohashGenerator = new GeohashGenerator();
    protected static final String STRING_FIELD_NAME = "mapped_string";
    protected static final String STRING_FIELD_NAME_2 = "mapped_string_2";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_FIELD_NAME = "mapped_date";
    protected static final String OBJECT_FIELD_NAME = "mapped_object";
    protected static final String GEO_POINT_FIELD_NAME = "mapped_geo_point";
    protected static final String GEO_SHAPE_FIELD_NAME = "mapped_geo_shape";
    protected static final String[] MAPPED_FIELD_NAMES = new String[] { STRING_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME,
            BOOLEAN_FIELD_NAME, DATE_FIELD_NAME, OBJECT_FIELD_NAME, GEO_POINT_FIELD_NAME, GEO_SHAPE_FIELD_NAME };
    protected static final String[] MAPPED_LEAF_FIELD_NAMES = new String[] { STRING_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME,
            BOOLEAN_FIELD_NAME, DATE_FIELD_NAME, GEO_POINT_FIELD_NAME };

    private static Injector injector;
    private static IndexQueryParserService queryParserService;

    protected static IndexQueryParserService queryParserService() {
        return queryParserService;
    }

    private static Index index;

    protected static Index getIndex() {
        return index;
    }

    private static String[] currentTypes;

    protected static String[] getCurrentTypes() {
        return currentTypes;
    }

    private static NamedWriteableRegistry namedWriteableRegistry;

    private static String[] randomTypes;
    private static ClientInvocationHandler clientInvocationHandler = new ClientInvocationHandler();

    /**
     * Setup for the whole base test class.
     */
    @BeforeClass
    public static void init() throws IOException {
        // we have to prefer CURRENT since with the range of versions we support it's rather unlikely to get the current actually.
        Version version = randomBoolean() ? Version.CURRENT : VersionUtils.randomVersionBetween(random(), Version.V_2_0_0_beta1, Version.CURRENT);
        Settings settings = Settings.settingsBuilder()
                .put("name", AbstractQueryTestCase.class.toString())
                .put("path.home", createTempDir())
                .build();
        Settings indexSettings = Settings.settingsBuilder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        index = new Index(randomAsciiOfLengthBetween(1, 10));
        final TestClusterService clusterService = new TestClusterService();
        clusterService.setState(new ClusterState.Builder(clusterService.state()).metaData(new MetaData.Builder().put(
                new IndexMetaData.Builder(index.name()).settings(indexSettings).numberOfShards(1).numberOfReplicas(0))));
        final Client proxy = (Client) Proxy.newProxyInstance(
                Client.class.getClassLoader(),
                new Class[]{Client.class},
                clientInvocationHandler);
        injector = new ModulesBuilder().add(
                new EnvironmentModule(new Environment(settings)),
                new SettingsModule(settings),
                new ThreadPoolModule(new ThreadPool(settings)),
                new IndicesModule(settings) {
                    @Override
                    public void configure() {
                        // skip services
                        bindQueryParsersExtension();
                    }
                },
                new ScriptModule(settings) {
                    @Override
                    protected void configure() {
                        Settings settings = Settings.builder()
                                .put("path.home", createTempDir())
                                // no file watching, so we don't need a ResourceWatcherService
                                .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING, false)
                                .build();
                        MockScriptEngine mockScriptEngine = new MockScriptEngine();
                        Multibinder<ScriptEngineService> multibinder = Multibinder.newSetBinder(binder(), ScriptEngineService.class);
                        multibinder.addBinding().toInstance(mockScriptEngine);
                        try {
                            Class.forName("com.github.mustachejava.Mustache");
                        } catch(ClassNotFoundException e) {
                            throw new IllegalStateException("error while loading mustache", e);
                        }
                        MustacheScriptEngineService mustacheScriptEngineService = new MustacheScriptEngineService(settings);
                        Set<ScriptEngineService> engines = new HashSet<>();
                        engines.add(mockScriptEngine);
                        engines.add(mustacheScriptEngineService);
                        List<ScriptContext.Plugin> customContexts = new ArrayList<>();
                        bind(ScriptContextRegistry.class).toInstance(new ScriptContextRegistry(customContexts));
                        try {
                            ScriptService scriptService = new ScriptService(settings, new Environment(settings), engines, null, new ScriptContextRegistry(customContexts));
                            bind(ScriptService.class).toInstance(scriptService);
                        } catch(IOException e) {
                            throw new IllegalStateException("error while binding ScriptService", e);
                        }


                    }
                },
                new IndexSettingsModule(index, indexSettings),
                new IndexCacheModule(indexSettings),
                new AnalysisModule(indexSettings, new IndicesAnalysisService(indexSettings)),
                new SimilarityModule(index, indexSettings),
                new IndexNameModule(index),
        new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(Client.class).toInstance(proxy);
                        Multibinder.newSetBinder(binder(), ScoreFunctionParser.class);
                        bind(ScoreFunctionParserMapper.class).asEagerSingleton();
                        bind(ClusterService.class).toProvider(Providers.of(clusterService));
                        bind(CircuitBreakerService.class).to(NoneCircuitBreakerService.class);
                        bind(NamedWriteableRegistry.class).asEagerSingleton();
                    }
                }
        ).createInjector();
        queryParserService = injector.getInstance(IndexQueryParserService.class);

        MapperService mapperService = queryParserService.mapperService;
        //create some random type with some default field, those types will stick around for all of the subclasses
        currentTypes = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < currentTypes.length; i++) {
            String type = randomAsciiOfLengthBetween(1, 10);
            mapperService.merge(type, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(type,
                    STRING_FIELD_NAME, "type=string",
                    STRING_FIELD_NAME_2, "type=string",
                    INT_FIELD_NAME, "type=integer",
                    DOUBLE_FIELD_NAME, "type=double",
                    BOOLEAN_FIELD_NAME, "type=boolean",
                    DATE_FIELD_NAME, "type=date",
                    OBJECT_FIELD_NAME, "type=object",
                    GEO_POINT_FIELD_NAME, "type=geo_point,lat_lon=true,geohash=true,geohash_prefix=true",
                    GEO_SHAPE_FIELD_NAME, "type=geo_shape"
            ).string()), false, false);
            // also add mappings for two inner field in the object field
            mapperService.merge(type, new CompressedXContent("{\"properties\":{\""+OBJECT_FIELD_NAME+"\":{\"type\":\"object\","
                    + "\"properties\":{\""+DATE_FIELD_NAME+"\":{\"type\":\"date\"},\""+INT_FIELD_NAME+"\":{\"type\":\"integer\"}}}}}"), false, false);
            currentTypes[i] = type;
        }
        namedWriteableRegistry = injector.getInstance(NamedWriteableRegistry.class);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        terminate(injector.getInstance(ThreadPool.class));
        injector = null;
        index = null;
        queryParserService = null;
        currentTypes = null;
        namedWriteableRegistry = null;
        randomTypes = null;
    }

    @Before
    public void beforeTest() {
        clientInvocationHandler.delegate = this;
        //set some random types to be queried as part the search request, before each test
        randomTypes = getRandomTypes();
    }

    protected void setSearchContext(String[] types) {
        TestSearchContext testSearchContext = new TestSearchContext();
        testSearchContext.setTypes(types);
        SearchContext.setCurrent(testSearchContext);
    }

    @After
    public void afterTest() {
        clientInvocationHandler.delegate = null;
        QueryShardContext.removeTypes();
        SearchContext.removeCurrent();
    }

    protected final QB createTestQueryBuilder() {
        QB query = doCreateTestQueryBuilder();
        //we should not set boost and query name for queries that don't parse it
        if (supportsBoostAndQueryName()) {
            if (randomBoolean()) {
                query.boost(2.0f / randomIntBetween(1, 20));
            }
            if (randomBoolean()) {
                query.queryName(randomAsciiOfLengthBetween(1, 10));
            }
        }
        return query;
    }

    /**
     * Create the query that is being tested
     */
    protected abstract QB doCreateTestQueryBuilder();

    /**
     * Generic test that creates new query from the test query and checks both for equality
     * and asserts equality on the two queries.
     */
    @Test
    public void testFromXContent() throws IOException {
        QB testQuery = createTestQueryBuilder();
        assertParsedQuery(testQuery.toString(), testQuery);
        for (Map.Entry<String, QB> alternateVersion : getAlternateVersions().entrySet()) {
            assertParsedQuery(alternateVersion.getKey(), alternateVersion.getValue());
        }
    }

    /**
     * Returns alternate string representation of the query that need to be tested as they are never used as output
     * of {@link QueryBuilder#toXContent(XContentBuilder, ToXContent.Params)}. By default there are no alternate versions.
     */
    protected Map<String, QB> getAlternateVersions() {
        return Collections.emptyMap();
    }

    /**
     * Parses the query provided as string argument and compares it with the expected result provided as argument as a {@link QueryBuilder}
     */
    protected final void assertParsedQuery(String queryAsString, QueryBuilder<?> expectedQuery) throws IOException {
        assertParsedQuery(queryAsString, expectedQuery, ParseFieldMatcher.STRICT);
    }

    protected final void assertParsedQuery(String queryAsString, QueryBuilder<?> expectedQuery, ParseFieldMatcher matcher) throws IOException {
        QueryBuilder<?> newQuery = parseQuery(queryAsString, matcher);
        assertNotSame(newQuery, expectedQuery);
        assertEquals(expectedQuery, newQuery);
        assertEquals(expectedQuery.hashCode(), newQuery.hashCode());
    }

    protected final QueryBuilder<?> parseQuery(String queryAsString) throws IOException {
        return parseQuery(queryAsString, ParseFieldMatcher.STRICT);
    }

    protected final QueryBuilder<?> parseQuery(String queryAsString, ParseFieldMatcher matcher) throws IOException {
        XContentParser parser = XContentFactory.xContent(queryAsString).createParser(queryAsString);
        return parseQuery(parser, matcher);
    }

    protected final QueryBuilder<?> parseQuery(BytesReference query) throws IOException {
        XContentParser parser = XContentFactory.xContent(query).createParser(query);
        return parseQuery(parser, ParseFieldMatcher.STRICT);
    }

    protected final QueryBuilder<?> parseQuery(XContentParser parser, ParseFieldMatcher matcher) throws IOException {
        QueryParseContext context = createParseContext();
        context.reset(parser);
        context.parseFieldMatcher(matcher);
        return context.parseInnerQueryBuilder();
    }

    /**
     * Test creates the {@link Query} from the {@link QueryBuilder} under test and delegates the
     * assertions being made on the result to the implementing subclass.
     */
    @Test
    public void testToQuery() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);

        QB firstQuery = createTestQueryBuilder();
        QB controlQuery = copyQuery(firstQuery);
        setSearchContext(randomTypes); // only set search context for toQuery to be more realistic
        Query firstLuceneQuery = firstQuery.toQuery(context);
        assertLuceneQuery(firstQuery, firstLuceneQuery, context);
        SearchContext.removeCurrent(); // remove after assertLuceneQuery since the assertLuceneQuery impl might access the context as well
        assertTrue("query is not equal to its copy after calling toQuery, firstQuery: " + firstQuery + ", secondQuery: " + controlQuery,
                firstQuery.equals(controlQuery));
        assertTrue("equals is not symmetric after calling toQuery, firstQuery: " + firstQuery + ", secondQuery: " + controlQuery,
                controlQuery.equals(firstQuery));
        assertThat("query copy's hashcode is different from original hashcode after calling toQuery, firstQuery: " + firstQuery
                + ", secondQuery: " + controlQuery, controlQuery.hashCode(), equalTo(firstQuery.hashCode()));


        QB secondQuery = copyQuery(firstQuery);
        //query _name never should affect the result of toQuery, we randomly set it to make sure
        if (randomBoolean()) {
            secondQuery.queryName(secondQuery.queryName() == null ? randomAsciiOfLengthBetween(1, 30) : secondQuery.queryName() + randomAsciiOfLengthBetween(1, 10));
        }
        setSearchContext(randomTypes); // only set search context for toQuery to be more realistic
        Query secondLuceneQuery = secondQuery.toQuery(context);
        assertLuceneQuery(secondQuery, secondLuceneQuery, context);
        SearchContext.removeCurrent(); // remove after assertLuceneQuery since the assertLuceneQuery impl might access the context as well

        assertThat("two equivalent query builders lead to different lucene queries", secondLuceneQuery, equalTo(firstLuceneQuery));

        //if the initial lucene query is null, changing its boost won't have any effect, we shouldn't test that
        if (firstLuceneQuery != null && supportsBoostAndQueryName()) {
            secondQuery.boost(firstQuery.boost() + 1f + randomFloat());
            setSearchContext(randomTypes); // only set search context for toQuery to be more realistic
            Query thirdLuceneQuery = secondQuery.toQuery(context);
            SearchContext.removeCurrent();
            assertThat("modifying the boost doesn't affect the corresponding lucene query", firstLuceneQuery, not(equalTo(thirdLuceneQuery)));
        }
    }

    /**
     * Few queries allow you to set the boost and queryName on the java api, although the corresponding parser doesn't parse them as they are not supported.
     * This method allows to disable boost and queryName related tests for those queries. Those queries are easy to identify: their parsers
     * don't parse `boost` and `_name` as they don't apply to the specific query: filter query, wrapper query and match_none
     */
    protected boolean supportsBoostAndQueryName() {
        return true;
    }

    /**
     * Checks the result of {@link QueryBuilder#toQuery(QueryShardContext)} given the original {@link QueryBuilder} and {@link QueryShardContext}.
     * Verifies that named queries and boost are properly handled and delegates to {@link #doAssertLuceneQuery(AbstractQueryBuilder, Query, QueryShardContext)}
     * for query specific checks.
     */
    protected final void assertLuceneQuery(QB queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedQueries().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo(query));
        }
        if (query != null) {
            assertBoost(queryBuilder, query);
        }
        doAssertLuceneQuery(queryBuilder, query, context);
    }

    /**
     * Allows to override boost assertions for queries that don't have the default behaviour
     */
    protected void assertBoost(QB queryBuilder, Query query) throws IOException {
        // workaround https://bugs.openjdk.java.net/browse/JDK-8056984
        float boost = queryBuilder.boost();
        assertThat(query.getBoost(), equalTo(boost));
    }

    /**
     * Checks the result of {@link QueryBuilder#toQuery(QueryShardContext)} given the original {@link QueryBuilder} and {@link QueryShardContext}.
     * Contains the query specific checks to be implemented by subclasses.
     */
    protected abstract void doAssertLuceneQuery(QB queryBuilder, Query query, QueryShardContext context) throws IOException;

    /**
     * Test serialization and deserialization of the test query.
     */
    @Test
    public void testSerialization() throws IOException {
        QB testQuery = createTestQueryBuilder();
        assertSerialization(testQuery);
    }

    /**
     * Serialize the given query builder and asserts that both are equal
     */
    @SuppressWarnings("unchecked")
    protected QB assertSerialization(QB testQuery) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            testQuery.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                QueryBuilder<?> prototype = queryParser(testQuery.getName()).getBuilderPrototype();
                QueryBuilder deserializedQuery = prototype.readFrom(in);
                assertEquals(deserializedQuery, testQuery);
                assertEquals(deserializedQuery.hashCode(), testQuery.hashCode());
                assertNotSame(deserializedQuery, testQuery);
                return (QB) deserializedQuery;
            }
        }
    }

    @Test
    public void testEqualsAndHashcode() throws IOException {
        QB firstQuery = createTestQueryBuilder();
        assertFalse("query is equal to null", firstQuery.equals(null));
        assertFalse("query is equal to incompatible type", firstQuery.equals(""));
        assertTrue("query is not equal to self", firstQuery.equals(firstQuery));
        assertThat("same query's hashcode returns different values if called multiple times", firstQuery.hashCode(), equalTo(firstQuery.hashCode()));

        QB secondQuery = copyQuery(firstQuery);
        assertTrue("query is not equal to self", secondQuery.equals(secondQuery));
        assertTrue("query is not equal to its copy", firstQuery.equals(secondQuery));
        assertTrue("equals is not symmetric", secondQuery.equals(firstQuery));
        assertThat("query copy's hashcode is different from original hashcode", secondQuery.hashCode(), equalTo(firstQuery.hashCode()));

        QB thirdQuery = copyQuery(secondQuery);
        assertTrue("query is not equal to self", thirdQuery.equals(thirdQuery));
        assertTrue("query is not equal to its copy", secondQuery.equals(thirdQuery));
        assertThat("query copy's hashcode is different from original hashcode", secondQuery.hashCode(), equalTo(thirdQuery.hashCode()));
        assertTrue("equals is not transitive", firstQuery.equals(thirdQuery));
        assertThat("query copy's hashcode is different from original hashcode", firstQuery.hashCode(), equalTo(thirdQuery.hashCode()));
        assertTrue("equals is not symmetric", thirdQuery.equals(secondQuery));
        assertTrue("equals is not symmetric", thirdQuery.equals(firstQuery));

        if (randomBoolean()) {
            secondQuery.queryName(secondQuery.queryName() == null ? randomAsciiOfLengthBetween(1, 30) : secondQuery.queryName()
                    + randomAsciiOfLengthBetween(1, 10));
        } else {
            secondQuery.boost(firstQuery.boost() + 1f + randomFloat());
        }
        assertThat("different queries should not be equal", secondQuery, not(equalTo(firstQuery)));
        assertThat("different queries should have different hashcode", secondQuery.hashCode(), not(equalTo(firstQuery.hashCode())));
    }

    private QueryParser<?> queryParser(String queryId) {
        return queryParserService.indicesQueriesRegistry().queryParsers().get(queryId);
    }

    //we use the streaming infra to create a copy of the query provided as argument
    protected QB copyQuery(QB query) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            query.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                QueryBuilder<?> prototype = queryParser(query.getName()).getBuilderPrototype();
                @SuppressWarnings("unchecked")
                QB secondQuery = (QB)prototype.readFrom(in);
                return secondQuery;
            }
        }
    }

    /**
     * @return a new {@link QueryShardContext} based on the base test index and queryParserService
     */
    protected static QueryShardContext createShardContext() {
        QueryShardContext queryCreationContext = new QueryShardContext(index, queryParserService);
        queryCreationContext.reset();
        queryCreationContext.parseFieldMatcher(ParseFieldMatcher.STRICT);
        return queryCreationContext;
    }

    /**
     * @return a new {@link QueryParseContext} based on the base test index and queryParserService
     */
    protected static QueryParseContext createParseContext() {
        QueryParseContext queryParseContext = new QueryParseContext(queryParserService.indicesQueriesRegistry());
        queryParseContext.reset(null);
        queryParseContext.parseFieldMatcher(ParseFieldMatcher.STRICT);
        return queryParseContext;
    }

    /**
     * create a random value for either {@link AbstractQueryTestCase#BOOLEAN_FIELD_NAME}, {@link AbstractQueryTestCase#INT_FIELD_NAME},
     * {@link AbstractQueryTestCase#DOUBLE_FIELD_NAME}, {@link AbstractQueryTestCase#STRING_FIELD_NAME} or
     * {@link AbstractQueryTestCase#DATE_FIELD_NAME}, or a String value by default
     */
    protected static Object getRandomValueForFieldName(String fieldName) {
        Object value;
        switch (fieldName) {
            case STRING_FIELD_NAME:
                if (rarely()) {
                    // unicode in 10% cases
                    JsonStringEncoder encoder = JsonStringEncoder.getInstance();
                    value = new String(encoder.quoteAsString(randomUnicodeOfLength(10)));
                } else {
                    value = randomAsciiOfLengthBetween(1, 10);
                }
                break;
            case INT_FIELD_NAME:
                value = randomIntBetween(0, 10);
                break;
            case DOUBLE_FIELD_NAME:
                value = randomDouble() * 10;
                break;
            case BOOLEAN_FIELD_NAME:
                value = randomBoolean();
                break;
            case DATE_FIELD_NAME:
                value = new DateTime(System.currentTimeMillis(), DateTimeZone.UTC).toString();
                break;
            default:
                value = randomAsciiOfLengthBetween(1, 10);
        }
        return value;
    }

    protected static String getRandomQueryText() {
        int terms = randomIntBetween(0, 3);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < terms; i++) {
            builder.append(randomAsciiOfLengthBetween(1, 10) + " ");
        }
        return builder.toString().trim();
    }

    /**
     * Helper method to return a mapped or a random field
     */
    protected String getRandomFieldName() {
        // if no type is set then return a random field name
        if (currentTypes == null || currentTypes.length == 0 || randomBoolean()) {
            return randomAsciiOfLengthBetween(1, 10);
        }
        return randomFrom(MAPPED_LEAF_FIELD_NAMES);
    }

    /**
     * Helper method to return a random field (mapped or unmapped) and a value
     */
    protected Tuple<String, Object> getRandomFieldNameAndValue() {
        String fieldName = getRandomFieldName();
        return new Tuple<>(fieldName, getRandomValueForFieldName(fieldName));
    }

    /**
     * Helper method to return a random rewrite method
     */
    protected static String getRandomRewriteMethod() {
        String rewrite;
        if (randomBoolean()) {
            rewrite = randomFrom(QueryParsers.CONSTANT_SCORE,
                    QueryParsers.SCORING_BOOLEAN,
                    QueryParsers.CONSTANT_SCORE_BOOLEAN).getPreferredName();
        } else {
            rewrite = randomFrom(QueryParsers.TOP_TERMS,
                    QueryParsers.TOP_TERMS_BOOST,
                    QueryParsers.TOP_TERMS_BLENDED_FREQS).getPreferredName() + "1";
        }
        return rewrite;
    }

    protected String[] getRandomTypes() {
        String[] types;
        if (currentTypes.length > 0 && randomBoolean()) {
            int numberOfQueryTypes = randomIntBetween(1, currentTypes.length);
            types = new String[numberOfQueryTypes];
            for (int i = 0; i < numberOfQueryTypes; i++) {
                types[i] = randomFrom(currentTypes);
            }
        } else {
            if (randomBoolean()) {
                types = new String[] { MetaData.ALL };
            } else {
                types = new String[0];
            }
        }
        return types;
    }

    protected String getRandomType() {
        return (currentTypes.length == 0) ? MetaData.ALL : randomFrom(currentTypes);
    }

    public static String randomGeohash(int minPrecision, int maxPrecision) {
        return geohashGenerator.ofStringLength(getRandom(), minPrecision, maxPrecision);
    }

    public static class GeohashGenerator extends CodepointSetGenerator {
        private final static char[] ASCII_SET = "0123456789bcdefghjkmnpqrstuvwxyz".toCharArray();

        public GeohashGenerator() {
            super(ASCII_SET);
        }
    }

    protected static Fuzziness randomFuzziness(String fieldName) {
        if (randomBoolean()) {
            return Fuzziness.fromEdits(randomIntBetween(0, 2));
        }
        if (randomBoolean()) {
            return Fuzziness.AUTO;
        }
        switch (fieldName) {
            case INT_FIELD_NAME:
                return Fuzziness.build(randomIntBetween(3, 100));
            case DOUBLE_FIELD_NAME:
                return Fuzziness.build(1 + randomFloat() * 10);
            case DATE_FIELD_NAME:
                return Fuzziness.build(randomTimeValue());
            default:
                return Fuzziness.AUTO;
        }
    }

    protected static boolean isNumericFieldName(String fieldName) {
        return INT_FIELD_NAME.equals(fieldName) || DOUBLE_FIELD_NAME.equals(fieldName);
    }

    protected static String randomAnalyzer() {
        return randomFrom("simple", "standard", "keyword", "whitespace");
    }

    protected static String randomMinimumShouldMatch() {
        return randomFrom("1", "-1", "75%", "-25%", "2<75%", "2<-25%");
    }

    protected static String randomTimeZone() {
        return randomFrom(TIMEZONE_IDS);
    }

    private static final List<String> TIMEZONE_IDS = new ArrayList<>(DateTimeZone.getAvailableIDs());

    private static class ClientInvocationHandler implements InvocationHandler {
        AbstractQueryTestCase delegate;
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.equals(Client.class.getDeclaredMethod("get", GetRequest.class))) {
                return new PlainActionFuture<GetResponse>() {
                    @Override
                    public GetResponse get() throws InterruptedException, ExecutionException {
                        return delegate.executeGet((GetRequest) args[0]);
                    }
                };
            } else if (method.equals(Client.class.getDeclaredMethod("multiTermVectors", MultiTermVectorsRequest.class))) {
                    return new PlainActionFuture<MultiTermVectorsResponse>() {
                        @Override
                        public MultiTermVectorsResponse get() throws InterruptedException, ExecutionException {
                            return delegate.executeMultiTermVectors((MultiTermVectorsRequest) args[0]);
                        }
                    };
            } else if (method.equals(Object.class.getDeclaredMethod("toString"))) {
                return "MockClient";
            }
            throw new UnsupportedOperationException("this test can't handle calls to: " + method);
        }

    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers / builders
     */
    protected GetResponse executeGet(GetRequest getRequest) {
        throw new UnsupportedOperationException("this test can't handle GET requests");
    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers / builders
     */
    protected MultiTermVectorsResponse executeMultiTermVectors(MultiTermVectorsRequest mtvRequest) {
        throw new UnsupportedOperationException("this test can't handle MultiTermVector requests");
    }

}
