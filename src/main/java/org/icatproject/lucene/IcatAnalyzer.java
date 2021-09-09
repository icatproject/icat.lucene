package org.icatproject.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * 
 * IcatAnalyzer class
 * 
 * The TokenStream enumerates the sequence of tokens, either from Fields of a Document or from query text.
 * Consists of following two subclasses:
 * 	Tokenizer, a TokenStream whose input is a Reader and
 * 	TokenFilter, a TokenStream whose input is another TokenStream.
 * StandardTokenizer implements the Word Break rules from the Unicode Text Segmentation algorithm
 * StandardFilter normalizes tokens extracted with StandardTokenizer.
 * 	EnglishPossessiveFilter removes possessives (trailing 's) from words.
 * 	LowerCaseFilter normalizes token text to lower case.
 * 	StopFilter removes stop words from a token stream.
 * 	PorterStemFilter transforms the token stream as per the Porter stemming algorithm.
 * 	Note: The input to the stemming filter must already be in lower case, so you will need to use LowerCaseFilter before.
 * 
 */

public class IcatAnalyzer extends Analyzer {
	
	/**
	 * @param fieldName
	 * @throws TokenStreamComponents(Tokenizer, TokenStream)
	 */
	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		Tokenizer source = new StandardTokenizer();
		TokenStream sink = new StandardFilter(source);
		sink = new EnglishPossessiveFilter(sink);
		sink = new LowerCaseFilter(sink);
		sink = new StopFilter(sink, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
		sink = new PorterStemFilter(sink);
		return new TokenStreamComponents(source, sink);
	}
}