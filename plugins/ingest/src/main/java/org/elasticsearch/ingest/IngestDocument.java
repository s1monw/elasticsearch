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

package org.elasticsearch.ingest;

import org.elasticsearch.common.Strings;

import java.util.*;

/**
 * Represents a single document being captured before indexing and holds the source and meta data (like id, type and index).
 */
public final class IngestDocument {
    /**
     * simonw: is there any chance that we model this as a flat hierarchy. ie there is a string key and a value but
     * but values are never maps again? I guess that is tricky with lists of objects... hmm, I see so
     * if we have to do this I'd really like to spend some time to model this properly and instead of
     * using Map<String, Object>... I need to first figure out how everything works...
     */
    private final Map<String, String> metaData;
    private final Map<String, Object> source;

    private boolean modified = false;

    public IngestDocument(String index, String type, String id, Map<String, Object> source) {
        this.metaData = new HashMap<>();
        this.metaData.put("_index", index);
        this.metaData.put("_type", type);
        this.metaData.put("_id", id);
        this.source = source;
    }

    public IngestDocument(IngestDocument other) {
        this.metaData = new HashMap<>(other.metaData);
        this.source = new HashMap<>(other.source);
    }

    /**
     * Returns the value contained in the document for the provided path
     * @param path The path within the document in dot-notation
     * @param clazz The expected class of the field value
     * @return the value for the provided path if existing, null otherwise
     * @throws IllegalArgumentException if the field is present but is not of the type provided as argument.
     */
    public <T> T getPropertyValue(String path, Class<T> clazz) {
        if (path == null || path.length() == 0) {
            return null;
        }
        String[] pathElements = Strings.splitStringToArray(path, '.');
        assert pathElements.length > 0;

        Map<String, Object> innerMap = getParent(pathElements);
        if (innerMap == null) {
            return null;
        }

        String leafKey = pathElements[pathElements.length - 1];
        Object property = innerMap.get(leafKey);
        if (property == null) {
            return null;
        }
        if (clazz.isInstance(property)) {
            return clazz.cast(property);
        }
        throw new IllegalArgumentException("field [" + path + "] of type [" + property.getClass().getName() + "] cannot be cast to [" + clazz.getName() + "]");
    }

    /**
     * Checks whether the document contains a value for the provided path
     * @param path The path within the document in dot-notation
     * @return true if the document contains a value for the property, false otherwise
     */
    public boolean hasPropertyValue(String path) {
        if (path == null || path.length() == 0) {
            return false;
        }
        String[] pathElements = Strings.splitStringToArray(path, '.');
        assert pathElements.length > 0;
        Map<String, Object> innerMap = getParent(pathElements);
        if (innerMap == null) {
            return false;
        }
        String leafKey = pathElements[pathElements.length - 1];
        return innerMap.containsKey(leafKey);
    }

    /**
     * Removes the property identified by the provided path
     * @param path the path of the property to be removed
     */
    public void removeProperty(String path) {
        if (path == null || path.length() == 0) {
            return;
        }
        String[] pathElements = Strings.splitStringToArray(path, '.');
        assert pathElements.length > 0;
        Map<String, Object> parent = getParent(pathElements);
        if (parent != null) {
            String leafKey = pathElements[pathElements.length - 1];
            if (parent.containsKey(leafKey)) {
                modified = true;
                parent.remove(leafKey);
            }
        }
    }

    private Map<String, Object> getParent(String[] pathElements) {
        Map<String, Object> innerMap = source;
        for (int i = 0; i < pathElements.length - 1; i++) {
            Object obj = innerMap.get(pathElements[i]);
            if (obj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> stringObjectMap = (Map<String, Object>) obj;
                innerMap = stringObjectMap;
            } else {
                return null;
            }
        }
        return innerMap;
    }

    /**
     * Sets the provided value to the provided path in the document.
     * Any non existing path element will be created.
     * @param path The path within the document in dot-notation
     * @param value The value to put in for the path key
     */
    public void setPropertyValue(String path, Object value) {
        if (path == null || path.length() == 0) {
            throw new IllegalArgumentException("cannot add null or empty field");
        }
        String[] pathElements = Strings.splitStringToArray(path, '.');
        assert pathElements.length > 0;

        Map<String, Object> inner = source;
        for (int i = 0; i < pathElements.length - 1; i++) {
            String pathElement = pathElements[i];
            if (inner.containsKey(pathElement)) {
                Object object = inner.get(pathElement);
                if (object instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> stringObjectMap = (Map<String, Object>) object;
                    inner = stringObjectMap;
                } else if (object == null ) {
                    throw new IllegalArgumentException("cannot add field to null parent, [" + Map.class.getName() + "] expected instead.");
                } else {
                    throw new IllegalArgumentException("cannot add field to parent [" + pathElement + "] of type [" + object.getClass().getName() + "], [" + Map.class.getName() + "] expected instead.");
                }
            } else {
                Map<String, Object> newInnerMap = new HashMap<>();
                inner.put(pathElement, newInnerMap);
                inner = newInnerMap;
            }
        }

        String leafKey = pathElements[pathElements.length - 1];
        inner.put(leafKey, value);
        modified = true;
    }

    public String getMetadata(MetaData metaData) {
        return this.metaData.get(metaData.getName());
    }

    /**
     * Returns the document. Should be used only for reading. Any change made to the returned map will
     * not be reflected to the modified flag. Modify the document instead using {@link #setPropertyValue(String, Object)}
     * and {@link #removeProperty(String)}
     */
    public Map<String, Object> getSource() {
        return source;
    }

    public boolean isModified() {
        return modified;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) { return true; }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        IngestDocument other = (IngestDocument) obj;
        return Objects.equals(source, other.source) &&
                Objects.equals(metaData, other.metaData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metaData, source);
    }

    public enum MetaData {

        INDEX("_index"),
        TYPE("_type"),
        ID("_id"),
        ROUTING("_routing"),
        PARENT("_parent"),
        TIMESTAMP("_timestamp"),
        TTL("_ttl");

        private final String name;

        MetaData(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

}
