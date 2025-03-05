package icat.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.icatproject.lucene.IcatAnalyzer;
import org.icatproject.lucene.IcatSynonymAnalyzer;
import org.icatproject.lucene.Lucene;
import org.icatproject.lucene.SearchBucket;
import org.icatproject.lucene.SearchBucket.SearchType;
import org.icatproject.lucene.exceptions.LuceneException;
import org.junit.Test;

import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;

public class TestLucene {

	static final int scale = (int) 1.0e5;

	private final FacetsConfig facetsConfig = new FacetsConfig();

	@Test
	public void testIcatAnalyzer() throws Exception {
		final String text = "This is a demo   of the 1st (or is it number 2) all singing and dancing TokenStream's API with added aardvarks";
		int n = 0;
		String newString = "";

		try (Analyzer analyzer = new IcatAnalyzer()) {
			TokenStream stream = analyzer.tokenStream("field", new StringReader(text));
			CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
			try {
				stream.reset(); // Curiously this is required
				while (stream.incrementToken()) {
					n++;
					newString = newString + " " + termAtt;
				}
				stream.end();
			} finally {
				stream.close();
			}
		}

		assertEquals(12, n);
		assertEquals(" demo of 1st number 2 all sing danc tokenstream api ad aardvark", newString);
	}

	/**
	 * Test that IcatSynonymAnalyzer injects stems for alternate spellings and
	 * chemical symbols for the elements 
	 */
	@Test
	public void testIcatSynonymAnalyzer() throws Exception {
		final String text = "hydrogen Helium LITHIUM be B NE ionisation TIME of FLIGHT technique ArPeS";
		int n = 0;
		String newString = "";

		try (Analyzer analyzer = new IcatSynonymAnalyzer()) {
			TokenStream stream = analyzer.tokenStream("field", new StringReader(text));
			CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
			try {
				stream.reset(); // Curiously this is required
				while (stream.incrementToken()) {
					n++;
					newString = newString + " " + termAtt;
				}
				stream.end();
			} finally {
				stream.close();
			}
		}

		assertEquals(24, n);
		assertEquals(" h hydrogen he helium li lithium beryllium be boron b neon ne ioniz ionis tof time of flight techniqu arp angl resolv photoemiss spectroscopi", newString);
	}

	/**
	 * Test that we do not stop words that are chemical symbols (As At Be In No)
	 * but otherwise filter out stop words
	 */
	@Test
	public void testIcatAnalyzerStopWords() throws Exception {
		final String text = "as at be in no that the their then there";
		int n = 0;
		String newString = "";

		try (Analyzer analyzer = new IcatAnalyzer()) {
			TokenStream stream = analyzer.tokenStream("field", new StringReader(text));
			CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
			try {
				stream.reset(); // Curiously this is required
				while (stream.incrementToken()) {
					n++;
					newString = newString + " " + termAtt;
				}
				stream.end();
			} finally {
				stream.close();
			}
		}

		assertEquals(5, n);
		assertEquals(" as at be in no", newString);
	}

	@Test
	public void testJoins() throws Exception {
		Analyzer analyzer = new IcatAnalyzer();
		IndexWriterConfig config;

		Path tmpLuceneDir = Files.createTempDirectory("lucene");
		FSDirectory investigationDirectory = FSDirectory.open(tmpLuceneDir.resolve("Investigation"));
		config = new IndexWriterConfig(analyzer);
		config.setOpenMode(OpenMode.CREATE);
		IndexWriter investigationWriter = new IndexWriter(investigationDirectory, config);

		FSDirectory investigationUserDirectory = FSDirectory.open(tmpLuceneDir.resolve("InvestigationUser"));
		config = new IndexWriterConfig(analyzer);
		config.setOpenMode(OpenMode.CREATE);
		IndexWriter investigationUserWriter = new IndexWriter(investigationUserDirectory, config);

		FSDirectory datasetDirectory = FSDirectory.open(tmpLuceneDir.resolve("Dataset"));
		config = new IndexWriterConfig(analyzer);
		config.setOpenMode(OpenMode.CREATE);
		IndexWriter datasetWriter = new IndexWriter(datasetDirectory, config);

		FSDirectory datafileDirectory = FSDirectory.open(tmpLuceneDir.resolve("Datafile"));
		config = new IndexWriterConfig(analyzer);
		config.setOpenMode(OpenMode.CREATE);
		IndexWriter datafileWriter = new IndexWriter(datafileDirectory, config);

		addInvestigationUser(investigationUserWriter, "fred", 101);
		addInvestigationUser(investigationUserWriter, "bill", 101);
		addInvestigationUser(investigationUserWriter, "mary", 102);
		addInvestigation(investigationWriter, "inv1", 101);
		addInvestigation(investigationWriter, "inv2", 102);
		addDataset(datasetWriter, "ds1", 1, 101);
		addDataset(datasetWriter, "ds2", 2, 101);
		addDataset(datasetWriter, "ds3", 3, 102);
		addDatafile(datafileWriter, "df1", 1, 1);
		addDatafile(datafileWriter, "df2", 2, 1);
		addDatafile(datafileWriter, "df3", 3, 2);
		addDatafile(datafileWriter, "df4", 4, 2);
		addDatafile(datafileWriter, "df5", 5, 2);

		for (int i = 0; i < scale; i++) {
			addInvestigationUser(investigationUserWriter, "extra" + i, 500 + i);
			addInvestigation(investigationWriter, "extra" + i, 500 + i);
		}

		investigationWriter.close();
		investigationUserWriter.close();
		datasetWriter.close();
		datafileWriter.close();

		IndexSearcher investigationSearcher = new IndexSearcher(DirectoryReader.open(investigationDirectory));
		IndexSearcher investigationUserSearcher = new IndexSearcher(DirectoryReader.open(investigationUserDirectory));
		IndexSearcher datasetSearcher = new IndexSearcher(DirectoryReader.open(datasetDirectory));
		IndexSearcher datafileSearcher = new IndexSearcher(DirectoryReader.open(datafileDirectory));

		StandardQueryParser parser = new StandardQueryParser();
		StandardQueryConfigHandler qpConf = (StandardQueryConfigHandler) parser.getQueryConfigHandler();
		qpConf.set(ConfigurationKeys.ANALYZER, analyzer);
		qpConf.set(ConfigurationKeys.ALLOW_LEADING_WILDCARD, true);

		long start = System.currentTimeMillis();

		checkInvestigations(Arrays.asList(0), "inv1", null, investigationSearcher, investigationUserSearcher, parser);
		checkInvestigations(Arrays.asList(1), "inv2", null, investigationSearcher, investigationUserSearcher, parser);
		checkInvestigations(Arrays.asList(0, 1), "inv*", null, investigationSearcher, investigationUserSearcher,
				parser);
		checkInvestigations(Arrays.asList(), "inv3", null, investigationSearcher, investigationUserSearcher, parser);
		checkInvestigations(Arrays.asList(), "inv1", "mary", investigationSearcher, investigationUserSearcher, parser);
		checkInvestigations(Arrays.asList(1), "inv2", "mary", investigationSearcher, investigationUserSearcher, parser);

		checkInvestigations(Arrays.asList(1), null, "mary", investigationSearcher, investigationUserSearcher, parser);
		checkInvestigations(Arrays.asList(0), null, "fred", investigationSearcher, investigationUserSearcher, parser);
		checkInvestigations(Arrays.asList(), null, "harry", investigationSearcher, investigationUserSearcher, parser);

		checkDatasets(Arrays.asList(2), "ds3", null, investigationSearcher, investigationUserSearcher, datasetSearcher,
				parser);
		checkDatasets(Arrays.asList(0, 1), null, "fred", investigationSearcher, investigationUserSearcher,
				datasetSearcher, parser);
		checkDatasets(Arrays.asList(2), "ds3", "mary", investigationSearcher, investigationUserSearcher,
				datasetSearcher, parser);
		checkDatasets(Arrays.asList(), "ds3", "fred", investigationSearcher, investigationUserSearcher, datasetSearcher,
				parser);

		checkDatafiles(Arrays.asList(0), "df1", null, investigationSearcher, investigationUserSearcher, datasetSearcher,
				datafileSearcher, parser);

		checkDatafiles(Arrays.asList(0), "df1", "fred", investigationSearcher, investigationUserSearcher,
				datasetSearcher, datafileSearcher, parser);

		checkDatafiles(Arrays.asList(0, 1, 2, 3, 4), null, "fred", investigationSearcher, investigationUserSearcher,
				datasetSearcher, datafileSearcher, parser);

		checkDatafiles(Arrays.asList(), null, "mary", investigationSearcher, investigationUserSearcher, datasetSearcher,
				datafileSearcher, parser);

		checkDatafiles(Arrays.asList(), "df1", "mary", investigationSearcher, investigationUserSearcher,
				datasetSearcher, datafileSearcher, parser);

		System.out.println("Join tests took " + (System.currentTimeMillis() - start) + "ms");
	}

	@Test
	public void testFacets() throws Exception {
		Analyzer analyzer = new IcatAnalyzer();
		IndexWriterConfig config;

		Path tmpLuceneDir = Files.createTempDirectory("lucene");
		FSDirectory investigationDirectory = FSDirectory.open(tmpLuceneDir.resolve("Investigation"));
		config = new IndexWriterConfig(analyzer);
		config.setOpenMode(OpenMode.CREATE);
		IndexWriter investigationWriter = new IndexWriter(investigationDirectory, config);

		// Add investigations with parameter and sample Facets
		addFacetedInvestigation(investigationWriter, "inv1", 101, "parameter1", "sample1");
		addFacetedInvestigation(investigationWriter, "inv2", 102, "parameter2", "sample2");

		// Add investigations with only the parameter Facet
		for (int i = 0; i < scale; i++) {
			addFacetedInvestigation(investigationWriter, "extra" + i, 500 + i, "parameter0");
		}

		investigationWriter.close();

		DirectoryReader directoryReader =  DirectoryReader.open(investigationDirectory);
		IndexSearcher investigationSearcher = new IndexSearcher(directoryReader);
		StandardQueryParser parser = new StandardQueryParser();
		StandardQueryConfigHandler qpConf = (StandardQueryConfigHandler) parser.getQueryConfigHandler();
		qpConf.set(ConfigurationKeys.ANALYZER, analyzer);
		qpConf.set(ConfigurationKeys.ALLOW_LEADING_WILDCARD, true);
		Map<String, Number> labelValuesParameter = new HashMap<>();
		Map<String, Number> labelValuesSample = new HashMap<>();

		long start = System.currentTimeMillis();

		// Get Facets that are relevant for "inv1"
		labelValuesParameter.put("parameter1", 1);
		labelValuesSample.put("sample1", 1);
		checkFacets(labelValuesParameter, labelValuesSample, "inv1", investigationSearcher, directoryReader, parser);
		
		// Get Facets that are relevant for "inv*"
		labelValuesParameter.put("parameter2", 1);
		labelValuesSample.put("sample2", 1);
		checkFacets(labelValuesParameter, labelValuesSample, "inv*", investigationSearcher, directoryReader, parser);
		
		// Get all Facets for "*"
		labelValuesParameter.put("parameter0", scale);
		checkFacets(labelValuesParameter, labelValuesSample, "*", investigationSearcher, directoryReader, parser);

		System.out.println("Facet tests took " + (System.currentTimeMillis() - start) + "ms");
	}

	@Test
	public void testLowercaseWildcard() throws Exception {
		Lucene lucene = new Lucene();
		checkQuery(lucene, "name:XX03RG9MTD1?", "+name:xx03rg9mtd1?");
		checkQuery(lucene, "name:XX03RG9MTD1*", "+name:xx03rg9mtd1*");
		checkQuery(lucene, "name:XX03RG9MTD1? name:processing", "+(name:xx03rg9mtd1? name:process)");
		checkQuery(lucene, "+name:XX03RG9MTD1? -name:processing", "+(+name:xx03rg9mtd1? -name:process)");
	}

	private void checkQuery(Lucene lucene, String text, String expected) throws LuceneException, IOException, QueryNodeException {
		JsonObjectBuilder builder = Json.createObjectBuilder();
		JsonObjectBuilder innerBuilder = Json.createObjectBuilder();
		innerBuilder.add("text", text);
		builder.add("query", innerBuilder);
		SearchBucket bucket = new SearchBucket(lucene, SearchType.DATAFILE, builder.build(), null, null);
		assertEquals(expected, bucket.query.toString());
	}


	private void checkDatafiles(List<Integer> dnums, String fname, String uname, IndexSearcher investigationSearcher,
			IndexSearcher investigationUserSearcher, IndexSearcher datasetSearcher, IndexSearcher datafileSearcher,
			StandardQueryParser parser) throws IOException, QueryNodeException {
		ScoreDoc[] hits = get(fname, uname, investigationSearcher, investigationUserSearcher, datasetSearcher,
				datafileSearcher, parser);
		assertEquals("Size", dnums.size(), hits.length);
		for (ScoreDoc hit : hits) {
			assertTrue("Found unexpected " + hit.doc, dnums.contains(hit.doc));
			assertNotNull(datafileSearcher.doc(hit.doc).get("id"));
		}
	}

	/* Datasets */
	private ScoreDoc[] get(String sname, String uname, IndexSearcher investigationSearcher,
			IndexSearcher investigationUserSearcher, IndexSearcher datasetSearcher, StandardQueryParser parser)
			throws IOException, QueryNodeException {
		BooleanQuery.Builder theQuery = new BooleanQuery.Builder();
		if (uname != null) {
			Query iuQuery = JoinUtil.createJoinQuery("investigation", false, "id",
					new TermQuery(new Term("name", uname)), investigationUserSearcher, ScoreMode.None);

			Query invQuery = JoinUtil.createJoinQuery("id", false, "investigation", iuQuery, investigationSearcher,
					ScoreMode.None);

			theQuery.add(invQuery, Occur.MUST);
		}

		if (sname != null) {
			theQuery.add(parser.parse(sname, "name"), Occur.MUST);
		}

		TopDocs topDocs = datasetSearcher.search(theQuery.build(), 50);
		return topDocs.scoreDocs;

	}

	/* Datafiles */
	private ScoreDoc[] get(String fname, String uname, IndexSearcher investigationSearcher,
			IndexSearcher investigationUserSearcher, IndexSearcher datasetSearcher, IndexSearcher datafileSearcher,
			StandardQueryParser parser) throws IOException, QueryNodeException {
		BooleanQuery.Builder theQuery = new BooleanQuery.Builder();
		if (uname != null) {
			Query iuQuery = JoinUtil.createJoinQuery("investigation", false, "id",
					new TermQuery(new Term("name", uname)), investigationUserSearcher, ScoreMode.None);

			Query invQuery = JoinUtil.createJoinQuery("id", false, "investigation", iuQuery, investigationSearcher,
					ScoreMode.None);

			Query dsQuery = JoinUtil.createJoinQuery("id", false, "dataset", invQuery, datasetSearcher, ScoreMode.None);

			theQuery.add(dsQuery, Occur.MUST);
		}

		if (fname != null) {
			theQuery.add(parser.parse(fname, "name"), Occur.MUST);
		}

		TopDocs topDocs = datafileSearcher.search(theQuery.build(), 50);
		return topDocs.scoreDocs;
	}

	/* Investigations */
	private ScoreDoc[] get(String iname, String uname, IndexSearcher investigationSearcher,
			IndexSearcher investigationUserSearcher, StandardQueryParser parser)
			throws QueryNodeException, IOException {
		BooleanQuery.Builder theQuery = new BooleanQuery.Builder();

		if (iname != null) {
			theQuery.add(parser.parse(iname, "name"), Occur.MUST);
		}

		if (uname != null) {
			Query iuQuery = JoinUtil.createJoinQuery("investigation", false, "id",
					new TermQuery(new Term("name", uname)), investigationUserSearcher, ScoreMode.None);
			theQuery.add(iuQuery, Occur.MUST);
		}

		TopDocs topDocs = investigationSearcher.search(theQuery.build(), 50);
		return topDocs.scoreDocs;

	}

	/* Facets */
	private Facets get(String iname, IndexSearcher investigationSearcher, DirectoryReader directoryReader,
			StandardQueryParser parser) throws QueryNodeException, IOException {
		BooleanQuery.Builder theQuery = new BooleanQuery.Builder();
		if (iname != null) {
			theQuery.add(parser.parse(iname, "name"), Occur.MUST);
		}
		DefaultSortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(directoryReader);
		FacetsCollector facetsCollector = new FacetsCollector();
		FacetsCollector.search(investigationSearcher, theQuery.build(), 50, facetsCollector);
		Facets facets = new SortedSetDocValuesFacetCounts(state, facetsCollector);
		return facets;
	}

	private void checkDatasets(List<Integer> dnums, String sname, String uname, IndexSearcher investigationSearcher,
			IndexSearcher investigationUserSearcher, IndexSearcher datasetSearcher, StandardQueryParser parser)
			throws IOException, QueryNodeException {
		ScoreDoc[] hits = get(sname, uname, investigationSearcher, investigationUserSearcher, datasetSearcher, parser);
		assertEquals("Size", dnums.size(), hits.length);
		for (ScoreDoc hit : hits) {
			assertTrue("Found unexpected " + hit.doc, dnums.contains(hit.doc));
			assertNotNull(datasetSearcher.doc(hit.doc).get("id"));
		}

	}

	private void checkFacets(Map<String, Number> labelValuesParameter, Map<String, Number> labelValuesSample,
			String iname, IndexSearcher investigationSearcher, DirectoryReader directoryReader,
			StandardQueryParser parser) throws QueryNodeException, IOException {
		Facets facets = get(iname, investigationSearcher, directoryReader, parser);
		List<FacetResult> results = facets.getAllDims(50);
		if (labelValuesParameter.size() > 0) {
			FacetResult parameterResult = results.remove(0);
			assertEquals("Dimension", "parameter", parameterResult.dim);
			assertEquals("Length", labelValuesParameter.size(), parameterResult.labelValues.length);
			for (LabelAndValue labelValue : parameterResult.labelValues) {
				assertTrue("Label", labelValuesParameter.containsKey(labelValue.label));
				assertEquals("Value", labelValuesParameter.get(labelValue.label), labelValue.value);
			}
		}
		if (labelValuesSample.size() > 0) {
			FacetResult sampleResult = results.remove(0);
			assertEquals("Dimension", "sample", sampleResult.dim);
			assertEquals("Length", labelValuesSample.size(), sampleResult.labelValues.length);
			for (LabelAndValue labelValue : sampleResult.labelValues) {
				assertTrue("Label", labelValuesSample.containsKey(labelValue.label));
				assertEquals("Value", labelValuesSample.get(labelValue.label), labelValue.value);
			}
		}
	}

	private void checkInvestigations(List<Integer> dnums, String iname, String uname,
			IndexSearcher investigationSearcher, IndexSearcher investigationUserSearcher, StandardQueryParser parser)
			throws QueryNodeException, IOException {

		ScoreDoc[] hits = get(iname, uname, investigationSearcher, investigationUserSearcher, parser);
		assertEquals("Size", dnums.size(), hits.length);
		for (ScoreDoc hit : hits) {
			assertTrue("Found unexpected " + hit.doc, dnums.contains(hit.doc));
			assertNotNull(investigationSearcher.doc(hit.doc).get("id"));
		}
	}

	private void addInvestigation(IndexWriter iwriter, String name, long iNum) throws IOException {
		Document doc = new Document();
		doc.add(new StringField("name", name, Store.NO));
		doc.add(new SortedDocValuesField("id", new BytesRef(Long.toString(iNum))));
		doc.add(new StringField("id", Long.toString(iNum), Store.YES));
		iwriter.addDocument(doc);
	}

	private void addFacetedInvestigation(IndexWriter iwriter, String name, long iNum, String parameterValue,
			String sampleValue) throws IOException {
		Document doc = new Document();
		doc.add(new StringField("name", name, Store.NO));
		doc.add(new SortedDocValuesField("id", new BytesRef(Long.toString(iNum))));
		doc.add(new StringField("id", Long.toString(iNum), Store.YES));
		doc.add(new SortedSetDocValuesFacetField("parameter", parameterValue));
		doc.add(new SortedSetDocValuesFacetField("sample", sampleValue));
		iwriter.addDocument(facetsConfig.build(doc));
	}

	private void addFacetedInvestigation(IndexWriter iwriter, String name, long iNum, String parameterValue)
			throws IOException {
		Document doc = new Document();
		doc.add(new StringField("name", name, Store.NO));
		doc.add(new SortedDocValuesField("id", new BytesRef(Long.toString(iNum))));
		doc.add(new StringField("id", Long.toString(iNum), Store.YES));
		doc.add(new SortedSetDocValuesFacetField("parameter", parameterValue));
		iwriter.addDocument(facetsConfig.build(doc));
	}

	private void addInvestigationUser(IndexWriter iwriter, String name, long iNum) throws IOException {
		Document doc = new Document();
		doc.add(new StringField("name", name, Store.NO));
		doc.add(new SortedDocValuesField("investigation", new BytesRef(Long.toString(iNum))));
		iwriter.addDocument(doc);
	}

	private void addDataset(IndexWriter iwriter, String name, int sNum, int iNum) throws IOException {
		Document doc = new Document();
		doc.add(new StringField("name", name, Store.NO));
		doc.add(new StringField("id", Long.toString(sNum), Store.YES));
		doc.add(new SortedDocValuesField("id", new BytesRef(Long.toString(sNum))));
		doc.add(new StringField("investigation", Long.toString(iNum), Store.NO));
		iwriter.addDocument(doc);
	}

	private void addDatafile(IndexWriter iwriter, String name, int fNum, int sNum) throws IOException {
		Document doc = new Document();
		doc.add(new StringField("name", name, Store.NO));
		doc.add(new StringField("id", Long.toString(fNum), Store.YES));
		doc.add(new StringField("dataset", Long.toString(sNum), Store.NO));
		iwriter.addDocument(doc);
	}

}
