/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request for checking a user's privileges
 */
public final class GetUserPrivilegesRequest extends ActionRequest implements UserRequest {

    private String username;

    /**
     * Package level access for {@link GetUserPrivilegesRequestBuilder}.
     */
    GetUserPrivilegesRequest() {
    }

    public GetUserPrivilegesRequest(StreamInput in) throws IOException {
        super(in);
        this.username = in.readString();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * @return the username that this request applies to.
     */
    public String username() {
        return username;
    }

    /**
     * Set the username that the request applies to. Must not be {@code null}
     */
    public void username(String username) {
        this.username = username;
    }

    @Override
    public String[] usernames() {
        return new String[] { username };
    }

    /**
     * Always throws {@link UnsupportedOperationException} as this object should be deserialized using
     * the {@link #GetUserPrivilegesRequest(StreamInput)} constructor instead.
     */
    @Override
    @Deprecated
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("Use " + getClass() + " as Writeable not Streamable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
    }

}
