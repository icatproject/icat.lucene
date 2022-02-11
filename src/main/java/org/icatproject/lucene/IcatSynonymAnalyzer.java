package org.icatproject.lucene;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.text.ParseException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;

public class IcatSynonymAnalyzer extends Analyzer {

    private SynonymMap synonyms;

    public IcatSynonymAnalyzer() 
            throws IOException, ParseException {
        super();
        // Load synonyms from resource file
        InputStream in = IcatSynonymAnalyzer.class.getClassLoader().getResourceAsStream("synonym.txt");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        SolrSynonymParser parser = new SolrSynonymParser(true, true, new IcatAnalyzer());
        parser.parse(reader);
        synonyms = parser.build();
    }

	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		Tokenizer source = new StandardTokenizer();
		TokenStream sink = new EnglishPossessiveFilter(source);
		sink = new LowerCaseFilter(sink);
		sink = new StopFilter(sink, IcatAnalyzer.SCIENTIFIC_STOP_WORDS_SET);
		sink = new PorterStemFilter(sink);
        sink = new SynonymGraphFilter(sink, synonyms, false);
		return new TokenStreamComponents(source, sink);
	}
}
