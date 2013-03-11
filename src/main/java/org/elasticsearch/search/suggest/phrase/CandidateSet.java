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
package org.elasticsearch.search.suggest.phrase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class CandidateSet {
    public Candidate[] candidates;
    public final Candidate originalTerm;

    public CandidateSet(Candidate[] candidates, Candidate originalTerm) {
        this.candidates = candidates;
        this.originalTerm = originalTerm;
    }

    public void addCandidates(List<Candidate> candidates) {
        final Set<Candidate> set = new HashSet<Candidate>(candidates);
        for (int i = 0; i < this.candidates.length; i++) {
            set.add(this.candidates[i]);
        }
        this.candidates = set.toArray(new Candidate[set.size()]);
    }

    public void addOneCandidate(Candidate candidate) {
        Candidate[] candidates = new Candidate[this.candidates.length + 1];
        System.arraycopy(this.candidates, 0, candidates, 0, this.candidates.length);
        candidates[candidates.length - 1] = candidate;
        this.candidates = candidates;
    }

    @Override
    public String toString() {
        return "CandidateSet [candidates=" + Arrays.toString(candidates) + ", originalTerm=" + originalTerm + "]";
    }

}
