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
package org.elasticsearch.transport;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A connection profile describes how many connection are established to specific node for each of the available request types.
 * ({@link org.elasticsearch.transport.TransportRequestOptions.Type}). This allows to tailor a connection towards a specific usage.
 */
public final class ConnectionProfile {

    /**
     * A pre-built light connection profile that shares a single connection across all
     * types.
     */
    public static final ConnectionProfile LIGHT_PROFILE = new ConnectionProfile(
        Collections.singletonList(new ConnectionTypeHandle(0, 1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.PING,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STATE)), 1, null);

    private final List<ConnectionTypeHandle> handles;
    private final int numConnection;
    private final TimeValue connectTimeout;

    private ConnectionProfile(List<ConnectionTypeHandle> handles, int numConnections, TimeValue timeout) {
        this.handles = handles;
        this.numConnection = numConnections;
        connectTimeout = timeout;
    }

    /**
     * A builder to build a new {@link ConnectionProfile}
     */
    public static class Builder {
        private final List<ConnectionTypeHandle> handles = new ArrayList<>();
        private final Set<TransportRequestOptions.Type> addedTypes = EnumSet.noneOf(TransportRequestOptions.Type.class);
        private int offset = 0;
        private TimeValue connectTimeout;

        /**
         * Sets a connect timeout for this connection profile
         */
        public void setConnectTimeout(TimeValue timeout) {
            if (timeout.millis() < 0) {
                throw new IllegalArgumentException("timeout must be positive but was: " + timeout);
            }
            this.connectTimeout = timeout;
        }

        /**
         * Adds a number of connections for one or more types. Each type can only be added once.
         * @param numConnections the number of connections to use in the pool for the given connection types
         * @param types a set of types that should share the given number of connections
         */
        public void addConnections(int numConnections, TransportRequestOptions.Type... types) {
            if (types == null || types.length == 0) {
                throw new IllegalArgumentException("types must not be null");
            }
            for (TransportRequestOptions.Type type : types) {
                if (addedTypes.contains(type)) {
                    throw new IllegalArgumentException("type [" + type + "] is already registered");
                }
            }
            addedTypes.addAll(Arrays.asList(types));
            handles.add(new ConnectionTypeHandle(offset, numConnections, types));
            offset += numConnections;
        }

        public ConnectionProfile build() {
            EnumSet<TransportRequestOptions.Type> types = EnumSet.allOf(TransportRequestOptions.Type.class);
            types.removeAll(addedTypes);
            if (types.isEmpty() == false) {
                throw new IllegalStateException("not all types are added for this connection profile - missing types: " + types);
            }
            return new ConnectionProfile(Collections.unmodifiableList(handles), offset, connectTimeout);
        }

    }

    /**
     * Returns the connect timeout or <code>null</code> if no explicit timeout is set on this profile.
     */
    public TimeValue getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Returns the total number of connections for this profile
     */
    public int getNumConnection() {
        return numConnection;
    }

    /**
     * Returns the type handles for this connection profile
     */
    List<ConnectionTypeHandle> getHandles() {
        return Collections.unmodifiableList(handles);
    }

    /**
     * Connection type handle encapsulates the logic which connection
     */
    static final class ConnectionTypeHandle {
        public final int length;
        public final int offset;
        private final TransportRequestOptions.Type[] types;
        private final AtomicInteger counter = new AtomicInteger();

        private ConnectionTypeHandle(int offset, int length, TransportRequestOptions.Type... types) {
            this.length = length;
            this.offset = offset;
            this.types = types;
        }

        /**
         * Returns one of the channels out configured for this handle. The channel is selected in a round-robin
         * fashion.
         */
        <T> T getChannel(T[] channels) {
            assert channels.length >= offset + length : "illegal size: " + channels.length + " expected >= " + (offset + length);
            return channels[offset + Math.floorMod(counter.incrementAndGet(), length)];
        }

        /**
         * Returns all types for this handle
         */
        TransportRequestOptions.Type[] getTypes() {
            return types;
        }
    }

}
