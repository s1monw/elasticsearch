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
import org.apache.lucene.util.Attribute;

/**
 * This attribute can be used to mark a token as a stopword. Stopword aware
 * {@link TokenStream}s or consumers can decide to keep or modify a token based on the return value
 * of {@link #isStopword()} if the token is kept or modified.
 */
public interface StopwordAttribute extends Attribute {
    
    /**
     * Returns <code>true</code> iff the current term is a stopword. Otherwise <code>false</code>.
     */
    public boolean isStopword();
    
    /**
     * Marks this term as a stopword iff set to <code>true</code>.
     * @param isStopword marks this term as a stopword iff set to <code>true</code>.
     */
    public void setStopword(boolean isStopword);

}
