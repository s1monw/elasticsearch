/**
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
package org.elasticsearch.script.mustache;

import com.fasterxml.jackson.core.io.SegmentedStringWriter;
import com.fasterxml.jackson.core.util.BufferRecycler;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.StringWriter;
import java.util.Map;

/**
 * Main entry point handling template registration, compilation and
 * execution.
 *
 * Template handling is based on Mustache. Template handling is a two step
 * process: First compile the string representing the template, the resulting
 * {@link Mustache} object can then be re-used for subsequent executions.
 */
public class MustacheScriptEngineService extends AbstractComponent implements ScriptEngineService {

    /** Factory to generate Mustache objects from. */
    private static final MustacheFactory MFACTORY = new DefaultMustacheFactory();

    /**
     * @param settings automatically wired by Guice.
     * */
    @Inject
    public MustacheScriptEngineService(Settings settings) {
        super(settings);
    }

    /**
     * Compile a template string to (in this case) a Mustache object than can
     * later be re-used for execution to fill in missing parameter values.
     *
     * @param template
     *            a string representing the template to compile.
     * @return a compiled template object for later execution.
     * */
    public Object compile(String template) {
        return MFACTORY.compile(new FastStringReader(template), "query-template");
    }

    /**
     * Execute a compiled template object (as retrieved from the compile method)
     * and fill potential place holders with the variables given.
     *
     * @param template
     *            compiled template object.
     * @param vars
     *            map of variables to use during substitution.
     *
     * @return the processed string with all given variables substitued.
     * */
    public Object execute(Object template, Map<String, Object> vars) {
        SegmentedStringWriter result = new SegmentedStringWriter(new BufferRecycler());
        ((Mustache) template).execute(result, vars);
        return result.getAndClear();
    }

    @Override
    public String[] types() {
        return new String[] {"mustache"};
    }

    @Override
    public String[] extensions() {
        return new String[] {"mustache"};
    }

    @Override
    public ExecutableScript executable(Object mustache,
            @Nullable Map<String, Object> vars) {
        return new MustacheExecutableScript((Mustache) mustache, vars);
    }

    @Override
    public SearchScript search(Object compiledScript, SearchLookup lookup,
            @Nullable Map<String, Object> vars) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object unwrap(Object value) {
        return value;
    }

    @Override
    public void close() {
        // Nothing to do here
    }

    /**
     * Used at query execution time by script service in order to execute a query template.
     * */
    private class MustacheExecutableScript implements ExecutableScript {
        /** Compiled template object. */
        private Mustache mustache;
        /** Parameters to fill above object with. */
        private Map<String, Object> vars;

        /**
         * @param mustache the compiled template object
         * @param vars the parameters to fill above object with
         **/
        public MustacheExecutableScript(Mustache mustache,
                Map<String, Object> vars) {
            this.mustache = mustache;
            this.vars = vars;
        }

        @Override
        public void setNextVar(String name, Object value) {
            this.vars.put(name, value);
        }

        @Override
        public Object run() {
            StringWriter result = new StringWriter();
            ((Mustache) mustache).execute(result, vars);
            return result.toString();
        }

        @Override
        public Object unwrap(Object value) {
            return value;
        }
    }
}
