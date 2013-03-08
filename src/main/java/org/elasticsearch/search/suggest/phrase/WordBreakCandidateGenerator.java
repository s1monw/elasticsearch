package org.elasticsearch.search.suggest.phrase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.swing.plaf.basic.BasicOptionPaneUI;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.BasicOperations;
import org.apache.lucene.util.automaton.SpecialOperations;
import org.apache.lucene.util.automaton.State;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.ElasticSearchIllegalArgumentException;

public class WordBreakCandidateGenerator extends CandidateGenerator {

    private final boolean useTotalTermFrequency;
    private final long dictSize;
    private final TermsEnum termsEnum;
    private final int minPrefix = 3;
    private final int minSuffix = 3;


    public WordBreakCandidateGenerator(IndexReader reader, String field) throws IOException {
        Terms terms = MultiFields.getTerms(reader, field);
        if (terms == null) {
            throw new ElasticSearchIllegalArgumentException("generator field [" + field + "] doesn't exist");
        }
        final long dictSize = terms.getSumTotalTermFreq();
        this.useTotalTermFrequency = dictSize != -1;
        this.dictSize =  dictSize == -1 ? reader.maxDoc() : dictSize;
        termsEnum = terms.iterator(null);
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
        Automaton prefix = BasicAutomata.makeString(termText, 0, minPrefix);
        Automaton suffix = BasicAutomata.makeString(termText, termText.length-minSuffix, minSuffix);
        State initial = new State();
        Automaton middle = new Automaton(initial);
        State current  = null;
        for (int i = minPrefix; i < termText.length-minSuffix; i++) {
            State next = new State();
            current.addTransition(new Transition(' ', next));
            current = next;
//            State build = build(initial, termText, i, termText.length-minSuffix-i);
//            build.
            
        }
        return set;
    }
    
    
    public static void main(String[] args) {
        String term = "helloworld";
        int[] termText = new int[term.codePointCount(0, term.length())];
        for (int cp, i = 0, j = 0; i < term.length(); i += Character.charCount(cp))
               termText[j++] = cp = term.codePointAt(i);
        State current = new State();
        build(current, termText, 5, termText.length-5);
        Automaton prefix = BasicAutomata.makeString(termText, 0, 5);
System.out.println("foo");
        Set<IntsRef> finiteStrings = SpecialOperations.getFiniteStrings(BasicOperations.concatenate(prefix, new Automaton(current)), 10000);
        for (IntsRef intsRef : finiteStrings) {
            for (int i = intsRef.offset; i < intsRef.length; i++) {
                System.out.print((char)intsRef.ints[i]);
            }
            System.out.println();
        }
    }
    
    
    public static void build(State current, int[] chars, int offset, int length) {
        State[] states = new State[length];
        State start = current;
        for (int i = 0; i < length; i++) {
            current = states[i] = addSingleChar(chars[offset+i], current);
        }
        current = start;
        for (int i = 0; i < length-1; i++) {
            current = addSingleChar(' ' , states[i]);
            for (int j = i; j < length; j++) {
                current = addSingleChar(chars[offset+j] , current);
            }
            current.setAccept(true);
        }
        
    }
    
    private static State addSingleChar(int character, State current) {
        State next = new State();
        current.addTransition(new Transition(character, next));
        return next;
    }

    @Override
    protected double score(long frequency, double errorScore) {
        return errorScore * (((double)frequency + 1) / ((double)dictSize +1));
    }

}
