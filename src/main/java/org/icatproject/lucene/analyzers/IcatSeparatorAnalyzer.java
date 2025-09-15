package org.icatproject.lucene.analyzers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.util.CharTokenizer;

public class IcatSeparatorAnalyzer extends Analyzer {

    private int code;

    public IcatSeparatorAnalyzer(String separator) {
        code = separator.codePointAt(0);
    }

    private boolean separatorCharPredicate(int x) {
        return x == code;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = CharTokenizer.fromSeparatorCharPredicate(this::separatorCharPredicate);
        TokenStream sink = new LowerCaseFilter(source);
        return new TokenStreamComponents(source, sink);
    }
}
