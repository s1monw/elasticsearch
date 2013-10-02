package org.apache.lucene.util;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Map;

/**
 * Abstract Visitor that supports visiting any kind of input data structure,
 * producing any kind of output.
 * <p/>
 * Implementations should provide methods named 'visit', accepting a subtype
 * of the specified input type, and returning subtypes of the specified output
 * type.
 * <p/>
 * The following is an example of how to visit various Query implementations:
 * <pre>
 * public class QueryVisitor&lt;Query, Query&gt; {
 *   public QueryVisitor() {
 *     super(QueryVisitor.class, Query.class, Query.class);
 *   }
 *   Query visit(TermQuery termQuery) { ... }
 *   Query visit(BooleanQuery booleanQuery) { ... }
 *   Query visit(Query query) { ... }
 * }
 * </pre>
 */
public abstract class Visitor<I, O> {

  private static final Map<Class<? extends Object>, InvocationDispatcher<Object, Object>> dispatcherByClass =
      new CopyOnWriteMap<Class<? extends Object>, InvocationDispatcher<Object, Object>>();

  protected final InvocationDispatcher<I, O> dispatcher;
  
  private static final String METHOD_NAME = "visit";

  /**
   * Creates a new Visitor that will visit methods in the given visitor class,
   * that accept parameters of the given input type, returning values of the
   * given output type
   *
   * @param visitorClass Class whose methods will be visited by the Visitor
   * @param inputType Type of the inputs to the visiting
   * @param outputType Type of the outputs of the visiting
   */
  @SuppressWarnings("unchecked")
  protected Visitor(Class<? extends Object> visitorClass, Class<I> inputType, Class<O> outputType) {
    InvocationDispatcher<I, O> dispatcher = (InvocationDispatcher<I, O>) dispatcherByClass.get(visitorClass);
    if (dispatcher == null) {
      dispatcher = new InvocationDispatcher<I, O>(visitorClass, METHOD_NAME, inputType, outputType);
      dispatcherByClass.put(visitorClass, (InvocationDispatcher<Object, Object>) dispatcher);
    }
    this.dispatcher = dispatcher;
  }

  /**
   * Applies the given input to the visitor
   *
   * @param input Input value to apply to the visitor
   * @return Return value from the visiting
   */
  public final O apply(I input) {
    return dispatcher.dispatch(this, input);
  }
}
