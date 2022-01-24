package org.icatproject.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
// import org.apache.lucene.analysis.standard.StandardAnalyzer ;
import org.apache.lucene.analysis.standard.StandardTokenizer;

// public class IcatAnalyzer extends Analyzer {

// 	@Override
// 	protected TokenStreamComponents createComponents(String fieldName) {
// 		StandardAnalyzer analyzer = new StandardAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
// 		Analyzer.TokenStreamComponents stream = analyzer.createComponents(fieldName);
// 		sink = new EnglishPossessiveFilter(stream.getTokenStream());
// 		sink = new PorterStemFilter(sink);
// 		return new TokenStreamComponents(source, sink);
// 	}
// }

public class IcatAnalyzer extends Analyzer {

	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		Tokenizer source = new StandardTokenizer();
		TokenStream sink = new EnglishPossessiveFilter(source);
		sink = new LowerCaseFilter(sink);
		sink = new StopFilter(sink, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
		sink = new PorterStemFilter(sink);
		return new TokenStreamComponents(source, sink);
	}
}