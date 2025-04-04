package org.icatproject.lucene;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class IcatAnalyzer extends Analyzer {

	public static final CharArraySet SCIENTIFIC_STOP_WORDS_SET;

	/**
	 * Do not include (As At Be In No) in the stop words as these are chemical
	 * symbols. Otherwise, the set should match Lucene's ENGLISH_STOP_WORDS_SET
	 */
	static {
		final List<String> stopWords =
			Arrays.asList("a", "an", "and", "are", "but", "by", "for", "if", "into", "is",
					"it", "not", "on", "or", "such", "that", "the", "their", "then",
					"there", "these", "they", "this", "to", "was", "will", "with");
		final CharArraySet stopSet = new CharArraySet(stopWords, false);
		SCIENTIFIC_STOP_WORDS_SET = CharArraySet.unmodifiableSet(stopSet);
	}

	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		Tokenizer source = new StandardTokenizer();
		TokenStream sink = new EnglishPossessiveFilter(source);
		sink = new LowerCaseFilter(sink);
		sink = new StopFilter(sink, SCIENTIFIC_STOP_WORDS_SET);
		sink = new PorterStemFilter(sink);
		return new TokenStreamComponents(source, sink);
	}
}