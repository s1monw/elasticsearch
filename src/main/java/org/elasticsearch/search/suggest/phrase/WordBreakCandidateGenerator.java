package org.elasticsearch.search.suggest.phrase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.SpecialOperations;
import org.apache.lucene.util.automaton.State;
import org.apache.lucene.util.automaton.Transition;
import org.elasticsearch.ElasticSearchIllegalArgumentException;

public class WordBreakCandidateGenerator extends CandidateGenerator {

    private final boolean useTotalTermFrequency;
    private final long dictSize;
    private final TermsEnum termsEnum;
    private final int prefixLength;
    private final int suffixLength;
    private final Terms terms;

    public WordBreakCandidateGenerator(IndexReader reader, String field, int prefixLength, int suffixLength) throws IOException {
        this.terms = MultiFields.getTerms(reader, field);
        if (terms == null) {
            throw new ElasticSearchIllegalArgumentException("generator field [" + field + "] doesn't exist");
        }
        final long dictSize = terms.getSumTotalTermFreq();
        this.useTotalTermFrequency = dictSize != -1;
        this.dictSize = dictSize == -1 ? reader.maxDoc() : dictSize;
        termsEnum = terms.iterator(null);
        this.prefixLength = prefixLength;
        this.suffixLength = suffixLength;
    }

    @Override
    public long frequency(BytesRef term) throws IOException {
        if (termsEnum.seekExact(term, true)) {
            return useTotalTermFrequency ? termsEnum.totalTermFreq() : termsEnum.docFreq();
        }
        return 0;
    }

    @Override
    public CandidateSet drawCandidates(CandidateSet set) throws IOException {
        Candidate original = set.originalTerm;
        final long frequency = original.frequency;
        String term = original.term.utf8ToString();
        int[] termText = new int[term.codePointCount(0, term.length())];
        for (int cp, i = 0, j = 0; i < term.length(); i += Character.charCount(cp))
            termText[j++] = cp = term.codePointAt(i);

        int prefixLength = getPrefixLength(termText.length);
        int suffixLength = getSuffixLength(termText.length);
        final long cutoffFrequency = getCutoffFrequency(frequency);
        final Automaton automaton = buildWordBreakAutomaton(termText, prefixLength, suffixLength);
        final CompiledAutomaton compiledAutomaton = new CompiledAutomaton(automaton);
        final TermsEnum termsEnum = new FilteredTermsEnum(compiledAutomaton.getTermsEnum(terms), false) {

            @Override
            protected AcceptStatus accept(BytesRef arg0) throws IOException {
                return this.totalTermFreq() > cutoffFrequency ? AcceptStatus.YES : AcceptStatus.NO;
            }
        };
        BytesRef termRef = new BytesRef();
        final List<Candidate> candidates = new ArrayList<Candidate>();
        while ((termRef = termsEnum.next()) != null) {
            long totalTermFreq = termsEnum.totalTermFreq();
            candidates.add(createCandidate(BytesRef.deepCopyOf(termRef), totalTermFreq, 1.0d - (1d / termRef.length)));
        }
        set.addCandidates(candidates);
        return set;
    }

    private long getCutoffFrequency(long frequency) {
        return frequency;
    }

    private int getSuffixLength(int length) {
        return suffixLength;
    }

    private int getPrefixLength(int length) {
        return prefixLength;
    }

    public static Automaton buildWordBreakAutomaton(int[] utf32CodePoints, int prefixLength, int suffixLength) {
        prefixLength = Math.max(0, prefixLength - 1);
        int wordBreakLength = utf32CodePoints.length - prefixLength - suffixLength;
        if (wordBreakLength <= 0) {
            return null;
        }
        State initialState = new State();
        State endPrefix = makeString(initialState, utf32CodePoints, 0, prefixLength);
        State initialSuffix = new State();
        State finalState = makeString(initialSuffix, utf32CodePoints, utf32CodePoints.length - suffixLength, suffixLength);
        finalState.setAccept(true);
        buildWordBreak(endPrefix, initialSuffix, utf32CodePoints, prefixLength, wordBreakLength);
        Automaton automaton = new Automaton(initialState);
        automaton.setDeterministic(true);
        return automaton;

    }

    public static State makeString(State s, int[] chars, int offset, int length) {
        for (int i = offset; i < offset + length; i++) {
            State s2 = new State();
            s.addTransition(new Transition(chars[i], s2));
            s = s2;
        }
        return s;
    }

    public static void buildWordBreak(State current, State suffix, int[] chars, int offset, int length) {
        State[] states = new State[length];
        State start = current;
        for (int i = 0; i < length; i++) {
            current = states[i] = addSingleChar(chars[offset + i], current);
        }
        current = start;
        for (int i = 0; i < length - 1; i++) {
            current = addSingleChar(' ', states[i]);

            for (int j = i + 1; j < length; j++) {
                if (j == length - 1) {
                    current.addTransition(new Transition(chars[offset + j], suffix));
                } else {
                    current = addSingleChar(chars[offset + j], current);
                }

            }
        }
        if (!suffix.isAccept()) {
            states[states.length - 1].addTransition(new Transition(' ', suffix));
        }
    }

    private static State addSingleChar(int character, State current) {
        State next = new State();
        current.addTransition(new Transition(character, next));
        return next;
    }

    @Override
    protected double score(long frequency, double errorScore) {
        return errorScore * (((double) frequency + 1) / ((double) dictSize + 1));
    }

}
