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

package org.apache.lucene.util;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.InvocationDispatcher.AmbigousMethodException;
import org.elasticsearch.test.ElasticSearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

public class VisitorTests extends ElasticSearchTestCase {
    private interface TopLevel {
    }

    private interface A extends TopLevel {
    }

    private interface B extends TopLevel {
    }

    private class AImpl implements A {
    }

    private class ABImpl implements A, B {
    }

    @Test
    public void testVisitorSimpleDispatch() {
        class AVisitor extends Visitor<A, String> {

            AVisitor() {
                super(AVisitor.class, A.class, String.class);
            }

            public String visit(A a) {
                return "value";
            }
        }

        Visitor<A, String> visitor = new AVisitor();
        String visitorOutput = visitor.apply(new AImpl());
        assertEquals("value", visitorOutput);
    }

    @Test
    public void testVisitorMultipleMethodsOnlyOneDispatchable() {
        class AVisitor extends Visitor<A, String> {

            AVisitor() {
                super(AVisitor.class, A.class, String.class);
            }

            public String visit(A a) {
                return "Visited A";
            }

            public String visit(B b) {
                return "Visited B";
            }
        }

        Visitor<A, String> visitor = new AVisitor();
        String visitorOutput = visitor.apply(new AImpl());
        assertEquals("Visited A", visitorOutput);
    }

    @Test(expected = AmbigousMethodException.class)
    public void testVisitorMultipleDispatchableMethods() {
        class TopLevelVisitor extends Visitor<TopLevel, String> {

            TopLevelVisitor() {
                super(TopLevelVisitor.class, TopLevel.class, String.class);
            }

            public String visit(A a) {
                return "Visited A";
            }

            public String visit(B b) {
                return "Visited B";
            }
        }

        Visitor<TopLevel, String> visitor = new TopLevelVisitor();
        visitor.apply(new ABImpl());
    }

    @Test
    public void testVisitorCatchAll() {
        class TopLevelVisitor extends Visitor<TopLevel, String> {

            TopLevelVisitor() {
                super(TopLevelVisitor.class, TopLevel.class, String.class);
            }

            String visit(B b) {
                return "Visited B";
            }

            protected String visit(TopLevel topLevel) {
                return "Visited Catchall";
            }
        }

        Visitor<TopLevel, String> visitor = new TopLevelVisitor();
        String visitorOutput = visitor.apply(new AImpl());
        assertEquals("Visited Catchall", visitorOutput);
    }
    
    
    @Test
    public void testConcreteVisitor() {
        class ConcreteVisitor extends Visitor<Query, Query> {

            ConcreteVisitor() {
                super(ConcreteVisitor.class, Query.class, Query.class);
            }
            
            Query visit(BooleanQuery query) {
                for (BooleanClause booleanClause : query) {
                  apply(booleanClause.getQuery());
                }
                return query;
              }

            Query visit(MultiTermQuery query) {
                query.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
                return query;
            }

            Query visit(PrefixQuery query) {
                query.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
                return query;
            }
        }
        
        PrefixQuery prefixQuery = new PrefixQuery(new Term("foo", "bar"));
        FuzzyQuery aMTQ = new FuzzyQuery(new Term("foo", "bar"));
        BooleanQuery q = new BooleanQuery();
        q.add(prefixQuery, Occur.MUST);
        q.add(aMTQ, Occur.MUST);
        new ConcreteVisitor().apply(q);
        assertThat(aMTQ.getRewriteMethod(), equalTo(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE));
        assertThat(prefixQuery.getRewriteMethod(), equalTo(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE));
    }
}
