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
import org.apache.lucene.util.AttributeImpl;

/**
 * Default implementation for the {@link StopwordAttribute}
 */
public class StopwordAttributeImpl extends AttributeImpl implements StopwordAttribute {
    private boolean stopword;

    /** Initialize this attribute with the stopword value as false. */
    public StopwordAttributeImpl() {
    }

    @Override
    public void clear() {
        stopword = false;
    }

    @Override
    public void copyTo(AttributeImpl target) {
        StopwordAttributeImpl attr = (StopwordAttributeImpl) target;
        attr.setStopword(stopword);
    }

    @Override
    public int hashCode() {
        return stopword ? 31 : 37;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (getClass() != obj.getClass())
            return false;
        final StopwordAttributeImpl other = (StopwordAttributeImpl) obj;
        return stopword == other.stopword;
    }

    public boolean isStopword() {
        return stopword;
    }

    public void setStopword(boolean isStopword) {
        stopword = isStopword;
    }
}
