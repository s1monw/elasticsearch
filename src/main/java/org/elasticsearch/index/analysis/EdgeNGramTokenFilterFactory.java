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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.*;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter.Side;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;


/**
 *
 */
public class EdgeNGramTokenFilterFactory extends AbstractTokenFilterFactory {

    private final int minGram;

    private final int maxGram;

    private final EdgeNGramTokenFilter.Side side;

    @Inject
    public EdgeNGramTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        this.minGram = settings.getAsInt("min_gram", NGramTokenFilter.DEFAULT_MIN_NGRAM_SIZE);
        this.maxGram = settings.getAsInt("max_gram", NGramTokenFilter.DEFAULT_MAX_NGRAM_SIZE);
        this.side = EdgeNGramTokenFilter.Side.getSide(settings.get("side", Lucene43EdgeNGramTokenizer.DEFAULT_SIDE.getLabel()));
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        final Version version = this.version == Version.LUCENE_43 ? Version.LUCENE_44 : this.version; // we supported it since 4.3
        if (version.onOrAfter(Version.LUCENE_43)) {
            TokenStream result = tokenStream;
            // side=BACK is not supported anymore but applying ReverseStringFilter up-front and after the token filter has the same effect
            if (side == Side.BACK) {
                result = new ReverseStringFilter(version, result);
            }
            result = new EdgeNGramTokenFilter(version, result, minGram, maxGram);
            if (side == Side.BACK) {
                result = new ReverseStringFilter(version, result);
            }
            return result;
        }
        return new EdgeNGramTokenFilter(version, tokenStream, side, minGram, maxGram);
    }
}