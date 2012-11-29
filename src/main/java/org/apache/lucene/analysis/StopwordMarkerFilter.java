package org.apache.lucene.analysis;
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
import java.io.IOException;

import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 * A filter similar to {@link StopFilter}. Yet, in contrast to {@link StopFilter} this filter doesn't
 * "drop" stopwords but marks them for as stopword for further consumption. 
 *
 */
public final class StopwordMarkerFilter extends TokenFilter {

    private final StopwordAttribute stopAttr = addAttribute(StopwordAttribute.class);
    private final CharArraySet stopWords;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    
    public StopwordMarkerFilter(TokenStream input, CharArraySet stopWords) {
        super(input);
        this.stopWords = stopWords;
    }
    
    @Override
    public final boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        stopAttr.setStopword(stopWords.contains(termAtt.buffer(), 0, termAtt.length()));
        return true;
      } else {
        return false;
      }
    }
}
