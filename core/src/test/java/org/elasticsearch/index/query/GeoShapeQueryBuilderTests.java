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

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class GeoShapeQueryBuilderTests extends AbstractQueryTestCase<GeoShapeQueryBuilder> {

    private static String indexedShapeId;
    private static String indexedShapeType;
    private static String indexedShapePath;
    private static String indexedShapeIndex;
    private static ShapeBuilder indexedShapeToReturn;

    @Override
    protected GeoShapeQueryBuilder doCreateTestQueryBuilder() {
        ShapeBuilder shape = RandomShapeGenerator.createShapeWithin(getRandom(), null);
        GeoShapeQueryBuilder builder;
        if (randomBoolean()) {
            try {
                builder = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, shape);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            indexedShapeToReturn = shape;
            indexedShapeId = randomAsciiOfLengthBetween(3, 20);
            indexedShapeType = randomAsciiOfLengthBetween(3, 20);
            builder = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, indexedShapeId, indexedShapeType);
            if (randomBoolean()) {
                indexedShapeIndex = randomAsciiOfLengthBetween(3, 20);
                builder.indexedShapeIndex(indexedShapeIndex);
            }
            if (randomBoolean()) {
                indexedShapePath = randomAsciiOfLengthBetween(3, 20);
                builder.indexedShapePath(indexedShapePath);
            }
        }
        if (randomBoolean()) {
            SpatialStrategy strategy = randomFrom(SpatialStrategy.values());
            builder.strategy(strategy);
            if (strategy != SpatialStrategy.TERM) {
                builder.relation(randomFrom(ShapeRelation.values()));
            }
        }
        return builder;
    }

    @Override
    protected GetResponse executeGet(GetRequest getRequest) {
        assertThat(indexedShapeToReturn, notNullValue());
        assertThat(indexedShapeId, notNullValue());
        assertThat(indexedShapeType, notNullValue());
        assertThat(getRequest.id(), equalTo(indexedShapeId));
        assertThat(getRequest.type(), equalTo(indexedShapeType));
        String expectedShapeIndex = indexedShapeIndex == null ? GeoShapeQueryBuilder.DEFAULT_SHAPE_INDEX_NAME : indexedShapeIndex;
        assertThat(getRequest.index(), equalTo(expectedShapeIndex));
        String expectedShapePath = indexedShapePath == null ? GeoShapeQueryBuilder.DEFAULT_SHAPE_FIELD_NAME : indexedShapePath;
        String json;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            builder.field(expectedShapePath, indexedShapeToReturn);
            builder.endObject();
            json = builder.string();
        } catch (IOException ex) {
            throw new ElasticsearchException("boom", ex);
        }
        return new GetResponse(new GetResult(indexedShapeIndex, indexedShapeType, indexedShapeId, 0, true, new BytesArray(json), null));
    }

    @After
    public void clearShapeFields() {
        indexedShapeToReturn = null;
        indexedShapeId = null;
        indexedShapeType = null;
        indexedShapePath = null;
        indexedShapeIndex = null;
    }

    @Override
    protected void doAssertLuceneQuery(GeoShapeQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        // Logic for doToQuery is complex and is hard to test here. Need to rely
        // on Integration tests to determine if created query is correct
        // TODO improve GeoShapeQueryBuilder.doToQuery() method to make it
        // easier to test here
        assertThat(query, anyOf(instanceOf(BooleanQuery.class), instanceOf(ConstantScoreQuery.class)));
    }

    /**
     * Overridden here to ensure the test is only run if at least one type is
     * present in the mappings. Geo queries do not execute if the field is not
     * explicitly mapped
     */
    @Override
    public void testToQuery() throws IOException {
        //TODO figure out why this test might take up to 10 seconds once in a while
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        super.testToQuery();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoFieldName() throws Exception {
        ShapeBuilder shape = RandomShapeGenerator.createShapeWithin(getRandom(), null);
        new GeoShapeQueryBuilder(null, shape);
    }

    @Test
    public void testNoShape() throws IOException {
        try {
            new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, (ShapeBuilder) null);
            fail("exception expected");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoIndexedShape() throws IOException {
        new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, null, "type");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoIndexedShapeType() throws IOException {
        new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, "id", null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testNoRelation() throws IOException {
        ShapeBuilder shape = RandomShapeGenerator.createShapeWithin(getRandom(), null);
        GeoShapeQueryBuilder builder = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, shape);
        builder.relation(null);
    }

    @Test
    public void testInvalidRelation() throws IOException {
        ShapeBuilder shape = RandomShapeGenerator.createShapeWithin(getRandom(), null);
        GeoShapeQueryBuilder builder = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, shape);
        try {
            builder.strategy(SpatialStrategy.TERM);
            builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.WITHIN));
            fail("Illegal combination of strategy and relation setting");
        } catch (IllegalArgumentException e) {
            // okay
        }

        try {
            builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.WITHIN));
            builder.strategy(SpatialStrategy.TERM);
            fail("Illegal combination of strategy and relation setting");
        } catch (IllegalArgumentException e) {
            // okay
        }
    }

    @Test // see #3878
    public void testThatXContentSerializationInsideOfArrayWorks() throws Exception {
        EnvelopeBuilder envelopeBuilder = ShapeBuilder.newEnvelope().topLeft(0, 0).bottomRight(10, 10);
        GeoShapeQueryBuilder geoQuery = QueryBuilders.geoShapeQuery("searchGeometry", envelopeBuilder);
        JsonXContent.contentBuilder().startArray().value(geoQuery).endArray();
    }
}
