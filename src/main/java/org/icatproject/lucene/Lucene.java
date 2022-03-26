package org.icatproject.lucene;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.Singleton;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.icatproject.lucene.exceptions.LuceneException;
import org.icatproject.utils.CheckedProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

@Path("/")
@Singleton
public class Lucene {

	enum AttributeName {
		type, name, value, date, store
	}

	enum FieldType {
		TextField, StringField, SortedDocValuesField, DoublePoint
	}

	private class IndexBucket {
		private FSDirectory directory;
		private IndexWriter indexWriter;
		private SearcherManager searcherManager;
		private AtomicBoolean locked = new AtomicBoolean();
	}

	public class Search {
		public Map<String, IndexSearcher> map;
		public Query query;
		public Sort sort;
		public Set<String> fields = new HashSet<String>();
	}

	enum When {
		Now, Sometime
	}

	private static final Logger logger = LoggerFactory.getLogger(Lucene.class);

	private static final Marker fatal = MarkerFactory.getMarker("FATAL");

	private java.nio.file.Path luceneDirectory;

	private int luceneCommitMillis;

	private AtomicLong bucketNum = new AtomicLong();
	private Map<String, IndexBucket> indexBuckets = new ConcurrentHashMap<>();
	private StandardQueryParser parser;

	private Timer timer;

	private IcatAnalyzer analyzer;

	private Map<Long, Search> searches = new ConcurrentHashMap<>();

	/**
	 * return the version of the lucene server
	 */
	@GET
	@Path("version")
	@Produces(MediaType.APPLICATION_JSON)
	public String getVersion() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonGenerator gen = Json.createGenerator(baos);
		gen.writeStartObject().write("version", Constants.API_VERSION).writeEnd();
		gen.close();
		return baos.toString();
	}

	/**
	 * Expect an array of things to add, update or delete to multiple documents
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("modify")
	public void modify(@Context HttpServletRequest request) throws LuceneException {

		logger.debug("Requesting modify");
		int count = 0;

		try (JsonParser parser = Json.createParser(request.getInputStream())) {

			Event ev = parser.next();
			if (ev != Event.START_ARRAY) {
				throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Unexpected " + ev.name());
			}
			ev = parser.next();

			while (true) {
				if (ev == Event.END_ARRAY) {
					break;
				}
				if (ev != Event.START_ARRAY) {
					throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Unexpected " + ev.name());
				}
				ev = parser.next();
				String entityName = parser.getString();
				ev = parser.next();
				Long id = (ev == Event.VALUE_NULL) ? null : parser.getLong();
				ev = parser.next();
				if (ev == Event.VALUE_NULL) {
					try {
						IndexBucket bucket = indexBuckets.computeIfAbsent(entityName, k -> createBucket(k));
						if (bucket.locked.get()) {
							throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
									"Lucene locked for " + entityName);
						}
						bucket.indexWriter.deleteDocuments(new Term("id", Long.toString(id)));
					} catch (IOException e) {
						throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
					}
				} else {
					add(request, entityName, When.Sometime, parser, id);
				}
				ev = parser.next(); // end of triple
				count++;
				ev = parser.next(); // either end of input or start of new
									// triple
			}

		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
		logger.debug("Modified {} documents", count);

	}

	/* if id is not null this is actually an update */
	private void add(HttpServletRequest request, String entityName, When when, JsonParser parser, Long id)
			throws LuceneException, IOException {

		IndexBucket bucket = indexBuckets.computeIfAbsent(entityName, k -> createBucket(k));

		AttributeName attName = null;
		FieldType fType = null;
		String name = null;
		String value = null;
		Double dvalue = null;
		Store store = Store.YES;
		Document doc = new Document();

		parser.next(); // Skip the [
		while (parser.hasNext()) {
			Event ev = parser.next();
			if (ev == Event.KEY_NAME) {
				try {
					attName = AttributeName.valueOf(parser.getString());
				} catch (Exception e) {
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
							"Found unknown field type " + e.getMessage());
				}
			} else if (ev == Event.VALUE_STRING) {
				if (attName == AttributeName.type) {
					try {
						fType = FieldType.valueOf(parser.getString());
					} catch (Exception e) {
						throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
								"Found unknown field type " + e.getMessage());
					}
				} else if (attName == AttributeName.name) {
					name = parser.getString();
				} else if (attName == AttributeName.value) {
					value = parser.getString();
				} else {
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST, "Bad VALUE_STRING " + attName);
				}
			} else if (ev == Event.VALUE_NUMBER) {
				long num = parser.getLong();
				if (fType == FieldType.SortedDocValuesField) {
					value = Long.toString(num);
				} else if (fType == FieldType.DoublePoint) {
					dvalue = parser.getBigDecimal().doubleValue();
				} else {
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
							"Bad VALUE_NUMBER " + attName + " " + fType);
				}
			} else if (ev == Event.VALUE_TRUE) {
				if (attName == AttributeName.store) {
					store = Store.YES;
				} else {
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST, "Bad VALUE_TRUE " + attName);
				}
			} else if (ev == Event.VALUE_FALSE) {
				if (attName == AttributeName.store) {
					store = Store.NO;
				} else {
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST, "Bad VALUE_FALSE " + attName);
				}
			} else if (ev == Event.START_OBJECT) {
				fType = null;
				name = null;
				value = null;
				store = Store.YES;
			} else if (ev == Event.END_OBJECT) {
				if (fType == FieldType.TextField) {
					doc.add(new TextField(name, value, store));
				} else if (fType == FieldType.StringField) {
					doc.add(new StringField(name, value, store));
				} else if (fType == FieldType.SortedDocValuesField) {
					// Any field we sort on must be stored to enable searching after
					doc.add(new SortedDocValuesField(name, new BytesRef(value)));
					doc.add(new StoredField(name, value));
				} else if (fType == FieldType.DoublePoint) {
					doc.add(new DoublePoint(name, dvalue));
					if (store == Store.YES) {
						doc.add(new StoredField(name, dvalue));
					}
				}
			} else if (ev == Event.END_ARRAY) {
				if (id == null) {
					if (bucket.locked.get() && when == When.Sometime) {
						throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
								"Lucene locked for " + entityName);
					}
					bucket.indexWriter.addDocument(doc);
				} else {
					if (bucket.locked.get()) {
						throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
								"Lucene locked for " + entityName);
					}
					bucket.indexWriter.updateDocument(new Term("id", id.toString()), doc);
				}
				return;
			} else {
				throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST, "Unexpected token in Json: " + ev);
			}
		}
	}

	/**
	 * Expect an array of documents each encoded as an array of things to add to
	 * the document
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("addNow/{entityName}")
	public void addNow(@Context HttpServletRequest request, @PathParam("entityName") String entityName)
			throws LuceneException {
		logger.debug("Requesting addNow of {}", entityName);
		int count = 0;
		try (JsonParser parser = Json.createParser(request.getInputStream())) {
			Event ev = parser.next(); // Opening [
			while (true) {
				ev = parser.next(); // Final ] or another document
				if (ev == Event.END_ARRAY) {
					break;
				}
				add(request, entityName, When.Now, parser, null);
				count++;
			}
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
		logger.debug("Added {} {} documents", count, entityName);
	}

	/*
	 * This is only for testing purposes. Other calls to the service will not
	 * work properly while this operation is in progress.
	 */
	@POST
	@Path("clear")
	public void clear() throws LuceneException {
		logger.info("Requesting clear");

		exit();
		timer = new Timer("LuceneCommitTimer");

		bucketNum.set(0);
		indexBuckets.clear();
		searches.clear();

		try {
			Files.walk(luceneDirectory, FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder())
					.filter(f -> !luceneDirectory.equals(f)).map(java.nio.file.Path::toFile).forEach(File::delete);
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}

		timer.schedule(new CommitTimerTask(), luceneCommitMillis, luceneCommitMillis);
		logger.info("clear complete - ready to go again");

	}

	@POST
	@Path("commit")
	public void commit() throws LuceneException {
		logger.debug("Requesting commit");
		try {
			for (Entry<String, IndexBucket> entry : indexBuckets.entrySet()) {
				IndexBucket bucket = entry.getValue();
				if (!bucket.locked.get()) {
					int cached = bucket.indexWriter.numRamDocs();
					bucket.indexWriter.commit();
					if (cached != 0) {
						logger.debug("Synch has committed {} {} changes to Lucene - now have {} documents indexed",
								cached, entry.getKey(), bucket.indexWriter.getDocStats().numDocs);
					}
					bucket.searcherManager.maybeRefreshBlocking();
				}
			}
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private IndexBucket createBucket(String name) {
		try {
			IndexBucket bucket = new IndexBucket();
			FSDirectory directory = FSDirectory.open(luceneDirectory.resolve(name));
			bucket.directory = directory;
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			IndexWriter iwriter = new IndexWriter(directory, config);
			String[] files = directory.listAll();
			if (files.length == 1 && files[0].equals("write.lock")) {
				logger.debug("Directory only has the write.lock file so store and delete a dummy document");
				Document doc = new Document();
				doc.add(new StringField("dummy", "dummy", Store.NO));
				iwriter.addDocument(doc);
				iwriter.commit();
				iwriter.deleteDocuments(new Term("dummy", "dummy"));
				iwriter.commit();
				logger.debug("Now have " + iwriter.getDocStats().numDocs + " documents indexed");
			}
			bucket.indexWriter = iwriter;
			bucket.searcherManager = new SearcherManager(iwriter, false, false, null);
			logger.debug("Bucket for {} is now ready", name);
			return bucket;
		} catch (Throwable e) {
			logger.error("Can't continue " + e.getClass() + " " + e.getMessage());
			return null;
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("datafiles")
	public String datafiles(@Context HttpServletRequest request, @QueryParam("search_after") String searchAfter,
			@QueryParam("maxResults") int maxResults, @QueryParam("sort") String sort) throws LuceneException {

		Long uid = null;
		try {
			uid = bucketNum.getAndIncrement();
			Search search = new Search();
			searches.put(uid, search);
			Map<String, IndexSearcher> map = new HashMap<>();
			search.map = map;
			search.sort = parseSort(sort);

			try (JsonReader r = Json.createReader(request.getInputStream())) {
				JsonObject o = r.readObject();
				JsonObject query = o.getJsonObject("query");
				String userName = query.getString("user", null);

				BooleanQuery.Builder theQuery = new BooleanQuery.Builder();

				if (userName != null) {
					Query iuQuery = JoinUtil.createJoinQuery("investigation", false, "id",
							new TermQuery(new Term("name", userName)), getSearcher(map, "InvestigationUser"),
							ScoreMode.None);

					Query invQuery = JoinUtil.createJoinQuery("id", false, "investigation", iuQuery,
							getSearcher(map, "Investigation"), ScoreMode.None);

					Query dsQuery = JoinUtil.createJoinQuery("id", false, "dataset", invQuery,
							getSearcher(map, "Dataset"), ScoreMode.None);

					theQuery.add(dsQuery, Occur.MUST);
				}

				String text = query.getString("text", null);
				if (text != null) {
					theQuery.add(parser.parse(text, "text"), Occur.MUST);
				}

				String lower = query.getString("lower", null);
				String upper = query.getString("upper", null);
				if (lower != null && upper != null) {
					theQuery.add(new TermRangeQuery("date", new BytesRef(lower), new BytesRef(upper), true, true),
							Occur.MUST);
				}

				if (query.containsKey("parameters")) {
					JsonArray parameters = query.getJsonArray("parameters");
					IndexSearcher datafileParameterSearcher = getSearcher(map, "DatafileParameter");
					for (JsonValue p : parameters) {
						BooleanQuery.Builder paramQuery = parseParameter(p);
						Query toQuery = JoinUtil.createJoinQuery("datafile", false, "id", paramQuery.build(),
								datafileParameterSearcher, ScoreMode.None);
						theQuery.add(toQuery, Occur.MUST);
					}
				}
				search.query = maybeEmptyQuery(theQuery);
				if (o.containsKey("fields")) {
					List<JsonString> jsonStrings = o.getJsonArray("fields").getValuesAs(JsonString.class);
					jsonStrings.forEach((jsonString) -> search.fields.add(jsonString.getString()));
				}
			}

			return luceneSearchResult("Datafile", search, searchAfter, maxResults, uid);
		} catch (Exception e) {
			logger.error("Error", e);
			freeSearcher(uid);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("datasets")
	public String datasets(@Context HttpServletRequest request, @QueryParam("search_after") String searchAfter,
			@QueryParam("maxResults") int maxResults, @QueryParam("sort") String sort) throws LuceneException {

		Long uid = null;
		try {
			uid = bucketNum.getAndIncrement();
			Search search = new Search();
			searches.put(uid, search);
			Map<String, IndexSearcher> map = new HashMap<>();
			search.map = map;
			search.sort = parseSort(sort);
			try (JsonReader r = Json.createReader(request.getInputStream())) {
				JsonObject o = r.readObject();
				JsonObject query = o.getJsonObject("query");
				String userName = query.getString("user", null);

				BooleanQuery.Builder theQuery = new BooleanQuery.Builder();

				if (userName != null) {

					Query iuQuery = JoinUtil.createJoinQuery("investigation", false, "id",
							new TermQuery(new Term("name", userName)), getSearcher(map, "InvestigationUser"),
							ScoreMode.None);

					Query invQuery = JoinUtil.createJoinQuery("id", false, "investigation", iuQuery,
							getSearcher(map, "Investigation"), ScoreMode.None);

					theQuery.add(invQuery, Occur.MUST);
				}

				String text = query.getString("text", null);
				if (text != null) {
					theQuery.add(parser.parse(text, "text"), Occur.MUST);
				}

				String lower = query.getString("lower", null);
				String upper = query.getString("upper", null);
				if (lower != null && upper != null) {
					theQuery.add(new TermRangeQuery("startDate", new BytesRef(lower), new BytesRef(upper), true, true),
							Occur.MUST);
					theQuery.add(new TermRangeQuery("endDate", new BytesRef(lower), new BytesRef(upper), true, true),
							Occur.MUST);
				}

				if (query.containsKey("parameters")) {
					JsonArray parameters = query.getJsonArray("parameters");
					IndexSearcher datasetParameterSearcher = getSearcher(map, "DatasetParameter");
					for (JsonValue p : parameters) {
						BooleanQuery.Builder paramQuery = parseParameter(p);
						Query toQuery = JoinUtil.createJoinQuery("dataset", false, "id", paramQuery.build(),
								datasetParameterSearcher, ScoreMode.None);
						theQuery.add(toQuery, Occur.MUST);
					}
				}
				search.query = maybeEmptyQuery(theQuery);
				if (o.containsKey("fields")) {
					List<JsonString> jsonStrings = o.getJsonArray("fields").getValuesAs(JsonString.class);
					jsonStrings.forEach((jsonString) -> search.fields.add(jsonString.getString()));
				}
			}
			return luceneSearchResult("Dataset", search, searchAfter, maxResults, uid);
		} catch (Exception e) {
			logger.error("Error", e);
			freeSearcher(uid);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}

	}

	@PreDestroy
	private void exit() {
		logger.info("Closing down icat.lucene");

		if (timer != null) {
			timer.cancel();
			timer = null; // This seems to be necessary to make it really stop
		}
		try {
			for (Entry<String, IndexBucket> entry : indexBuckets.entrySet()) {
				IndexBucket bucket = entry.getValue();
				bucket.searcherManager.close();
				bucket.indexWriter.commit();
				bucket.indexWriter.close();
				bucket.directory.close();
			}
			logger.info("Closed down icat.lucene");
		} catch (Exception e) {
			logger.error(fatal, "Problem closing down icat.lucene", e);
		}
	}

	@DELETE
	@Path("freeSearcher/{uid}")
	public void freeSearcher(@PathParam("uid") Long uid) throws LuceneException {
		if (uid != null) { // May not be set for internal calls
			logger.debug("Requesting freeSearcher {}", uid);
			Map<String, IndexSearcher> search = searches.get(uid).map;
			for (Entry<String, IndexSearcher> entry : search.entrySet()) {
				String name = entry.getKey();
				IndexSearcher isearcher = entry.getValue();
				SearcherManager manager = indexBuckets.computeIfAbsent(name, k -> createBucket(k)).searcherManager;
				try {
					manager.release(isearcher);
				} catch (IOException e) {
					throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
				}
			}
			searches.remove(uid);
		}
	}

	/*
	 * Need a new set of IndexSearchers for each search as identified by a uid
	 */
	private IndexSearcher getSearcher(Map<String, IndexSearcher> bucket, String name) throws IOException {
		IndexSearcher isearcher = bucket.get(name);
		if (isearcher == null) {
			isearcher = indexBuckets.computeIfAbsent(name, k -> createBucket(k)).searcherManager.acquire();
			bucket.put(name, isearcher);
			logger.debug("Remember searcher for {}", name);
		}
		return isearcher;
	}

	@PostConstruct
	private void init() {
		logger.info("Initialising icat.lucene");
		CheckedProperties props = new CheckedProperties();
		try {
			props.loadFromResource("run.properties");

			luceneDirectory = props.getPath("directory");
			if (!luceneDirectory.toFile().isDirectory()) {
				throw new Exception(luceneDirectory + " is not a directory");
			}

			luceneCommitMillis = props.getPositiveInt("commitSeconds") * 1000;

			analyzer = new IcatAnalyzer();

			parser = new StandardQueryParser();
			StandardQueryConfigHandler qpConf = (StandardQueryConfigHandler) parser.getQueryConfigHandler();
			qpConf.set(ConfigurationKeys.ANALYZER, analyzer);
			qpConf.set(ConfigurationKeys.ALLOW_LEADING_WILDCARD, true);

			timer = new Timer("LuceneCommitTimer");
			timer.schedule(new CommitTimerTask(), luceneCommitMillis, luceneCommitMillis);

		} catch (Exception e) {
			logger.error(fatal, e.getMessage());
			throw new IllegalStateException(e.getMessage());
		}

		logger.info("Initialised icat.lucene");
	}

	class CommitTimerTask extends TimerTask {
		@Override
		public void run() {
			try {
				commit();
			} catch (Throwable t) {
				logger.error(t.getMessage());
			}
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("investigations")
	public String investigations(@Context HttpServletRequest request, @QueryParam("search_after") String searchAfter,
			@QueryParam("maxResults") int maxResults, @QueryParam("sort") String sort) throws LuceneException {
		Long uid = null;
		try {
			uid = bucketNum.getAndIncrement();
			Search search = new Search();
			searches.put(uid, search);
			Map<String, IndexSearcher> map = new HashMap<>();
			search.map = map;
			search.sort = parseSort(sort);
			try (JsonReader r = Json.createReader(request.getInputStream())) {
				JsonObject o = r.readObject();
				JsonObject query = o.getJsonObject("query");
				String userName = query.getString("user", null);

				BooleanQuery.Builder theQuery = new BooleanQuery.Builder();

				if (userName != null) {
					Query iuQuery = JoinUtil.createJoinQuery("investigation", false, "id",
							new TermQuery(new Term("name", userName)), getSearcher(map, "InvestigationUser"),
							ScoreMode.None);
					theQuery.add(iuQuery, Occur.MUST);
				}

				String text = query.getString("text", null);
				if (text != null) {
					theQuery.add(parser.parse(text, "text"), Occur.MUST);
				}

				String lower = query.getString("lower", null);
				String upper = query.getString("upper", null);
				if (lower != null && upper != null) {
					theQuery.add(new TermRangeQuery("startDate", new BytesRef(lower), new BytesRef(upper), true, true),
							Occur.MUST);
					theQuery.add(new TermRangeQuery("endDate", new BytesRef(lower), new BytesRef(upper), true, true),
							Occur.MUST);
				}

				if (query.containsKey("parameters")) {
					JsonArray parameters = query.getJsonArray("parameters");
					IndexSearcher investigationParameterSearcher = getSearcher(map, "InvestigationParameter");

					for (JsonValue p : parameters) {
						BooleanQuery.Builder paramQuery = parseParameter(p);
						Query toQuery = JoinUtil.createJoinQuery("investigation", false, "id", paramQuery.build(),
								investigationParameterSearcher, ScoreMode.None);
						theQuery.add(toQuery, Occur.MUST);
					}
				}

				if (query.containsKey("samples")) {
					JsonArray samples = query.getJsonArray("samples");
					IndexSearcher sampleSearcher = getSearcher(map, "Sample");

					for (JsonValue s : samples) {
						JsonString sample = (JsonString) s;
						BooleanQuery.Builder sampleQuery = new BooleanQuery.Builder();
						sampleQuery.add(parser.parse(sample.getString(), "text"), Occur.MUST);
						Query toQuery = JoinUtil.createJoinQuery("investigation", false, "id", sampleQuery.build(),
								sampleSearcher, ScoreMode.None);
						theQuery.add(toQuery, Occur.MUST);
					}
				}

				String userFullName = query.getString("userFullName", null);
				if (userFullName != null) {
					BooleanQuery.Builder userFullNameQuery = new BooleanQuery.Builder();
					userFullNameQuery.add(parser.parse(userFullName, "text"), Occur.MUST);
					IndexSearcher investigationUserSearcher = getSearcher(map, "InvestigationUser");
					Query toQuery = JoinUtil.createJoinQuery("investigation", false, "id", userFullNameQuery.build(),
							investigationUserSearcher, ScoreMode.None);
					theQuery.add(toQuery, Occur.MUST);
				}

				search.query = maybeEmptyQuery(theQuery);
				if (o.containsKey("fields")) {
					List<JsonString> jsonStrings = o.getJsonArray("fields").getValuesAs(JsonString.class);
					jsonStrings.forEach((jsonString) -> search.fields.add(jsonString.getString()));
				}
			}
			logger.info("Query: {}", search.query);
			return luceneSearchResult("Investigation", search, searchAfter, maxResults, uid);
		} catch (Exception e) {
			logger.error("Error", e);
			freeSearcher(uid);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}

	}

	@POST
	@Path("lock/{entityName}")
	public void lock(@PathParam("entityName") String entityName) throws LuceneException {
		logger.info("Requesting lock of {} index", entityName);
		IndexBucket bucket = indexBuckets.computeIfAbsent(entityName, k -> createBucket(k));

		if (!bucket.locked.compareAndSet(false, true)) {
			throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE, "Lucene already locked for " + entityName);
		}
		try {
			bucket.indexWriter.deleteAll();
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private String luceneSearchResult(String name, Search search, String searchAfter, int maxResults, Long uid)
			throws IOException, LuceneException {
		IndexSearcher isearcher = getSearcher(search.map, name);
		logger.debug("To search in {} for {} {} with {} from {} ", name, search.query, maxResults, isearcher,
				searchAfter);
		FieldDoc searchAfterDoc = parseSearchAfter(searchAfter);
		ScoreDoc[] hits;
		TotalHits totalHits;
		SortField[] fields = null;
		if (search.sort == null) {
			// Use default score sorting
			TopDocs topDocs;
			topDocs = searchAfterDoc == null ? isearcher.search(search.query, maxResults)
					: isearcher.searchAfter(searchAfterDoc, search.query, maxResults);
			hits = topDocs.scoreDocs;
			totalHits = topDocs.totalHits;
		} else {
			// Use specified sorting
			TopFieldDocs topFieldDocs;
			topFieldDocs = searchAfterDoc == null ? isearcher.search(search.query, maxResults, search.sort)
					: isearcher.searchAfter(searchAfterDoc, search.query, maxResults, search.sort, false);
			hits = topFieldDocs.scoreDocs;
			totalHits = topFieldDocs.totalHits;
			fields = topFieldDocs.fields;
		}
		Float maxScore;
		if (hits.length == 0) {
			maxScore = Float.NaN;
		} else {
			maxScore = hits[0].score;
		}
		logger.debug("Hits " + totalHits + " maxscore " + maxScore);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (JsonGenerator gen = Json.createGenerator(baos)) {
			gen.writeStartObject();
			gen.writeStartArray("results");
			for (ScoreDoc hit : hits) {
				Document doc = isearcher.doc(hit.doc);
				gen.writeStartObject().write("id", Long.parseLong(doc.get("id")));
				Float score = hit.score;
				if (!score.equals(Float.NaN)) {
					gen.write("score", hit.score);
				}
				gen.writeStartObject("source");
				doc.forEach((field) -> {
					if (search.fields.contains(field.name())) {
						if (field.stringValue() != null) {
							gen.write(field.name(), field.stringValue());
						} else if (field.numericValue() != null) {
							gen.write(field.name(), field.numericValue().doubleValue());
						}
					}
				});
				gen.writeEnd();
				gen.writeEnd(); // result object
			}
			gen.writeEnd(); // array results
			if (hits.length == maxResults) {
				ScoreDoc lastDoc = hits[hits.length - 1];
				gen.writeStartObject("search_after").write("doc", lastDoc.doc).write("shardIndex", lastDoc.shardIndex);
				float lastScore = lastDoc.score;
				if (!Float.isNaN(lastScore)) {
					gen.write("score", lastScore);
				}
				if (fields != null) {
					Document lastDocument = isearcher.doc(lastDoc.doc);
					gen.writeStartArray("fields");
					for (SortField sortField : fields) {
						Type type = sortField.getType();
						if (type.equals(Type.STRING)) {
							String lastValue = lastDocument.get(sortField.getField());
							if (lastValue == null) {
								throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Field "
										+ sortField.getField()
										+ " used for sorting was not present on the Lucene Document; all sortable fields must also be stored.");
							}
							gen.write(lastValue);
						}
					}
					gen.writeEnd();
				}
				gen.writeEnd();
			}
			gen.writeEnd(); // object
		}
		logger.debug("Json returned {}", baos.toString());
		return baos.toString();
	}

	private Query maybeEmptyQuery(Builder theQuery) {
		Query query = theQuery.build();
		if (query.toString().isEmpty()) {
			query = new MatchAllDocsQuery();
		}
		logger.debug("Lucene query {}", query);
		return query;
	}

	private Builder parseParameter(JsonValue p) {
		JsonObject parameter = (JsonObject) p;
		BooleanQuery.Builder paramQuery = new BooleanQuery.Builder();
		String pName = parameter.getString("name", null);
		if (pName != null) {
			paramQuery.add(new WildcardQuery(new Term("name", pName)), Occur.MUST);
		}

		String pUnits = parameter.getString("units", null);
		if (pUnits != null) {
			paramQuery.add(new WildcardQuery(new Term("units", pUnits)), Occur.MUST);
		}
		String pStringValue = parameter.getString("stringValue", null);
		String pLowerDateValue = parameter.getString("lowerDateValue", null);
		String pUpperDateValue = parameter.getString("upperDateValue", null);
		Double pLowerNumericValue = parameter.containsKey("lowerNumericValue")
				? parameter.getJsonNumber("lowerNumericValue").doubleValue()
				: null;
		Double pUpperNumericValue = parameter.containsKey("upperNumericValue")
				? parameter.getJsonNumber("upperNumericValue").doubleValue()
				: null;
		if (pStringValue != null) {
			paramQuery.add(new WildcardQuery(new Term("stringValue", pStringValue)), Occur.MUST);
		} else if (pLowerDateValue != null && pUpperDateValue != null) {
			paramQuery.add(new TermRangeQuery("dateTimeValue", new BytesRef(pLowerDateValue),
					new BytesRef(pUpperDateValue), true, true), Occur.MUST);

		} else if (pLowerNumericValue != null && pUpperNumericValue != null) {
			paramQuery.add(DoublePoint.newRangeQuery("numericValue", pLowerNumericValue, pUpperNumericValue),
					Occur.MUST);
		}
		return paramQuery;
	}

	/**
	 * Parses the String from the request into a Lucene Sort object. Multiple sort
	 * criteria are supported, and will be applied in order.
	 * 
	 * @param sort String representation of a JSON object with the field(s) to sort
	 *             as keys, and the direction ("asc" or "desc") as value(s).
	 * @return Lucene Sort object
	 * @throws LuceneException If the value for any key isn't "asc" or "desc"
	 */
	private Sort parseSort(String sort) throws LuceneException {
		if (sort == null || sort.equals("")) {
			return null;
		}
		try (JsonReader reader = Json.createReader(new ByteArrayInputStream(sort.getBytes()))) {
			JsonObject object = reader.readObject();
			List<SortField> fields = new ArrayList<>();
			for (String key : object.keySet()) {
				String order = object.getString(key);
				Boolean reverse;
				if (order.equals("asc")) {
					reverse = false;
				} else if (order.equals("desc")) {
					reverse = true;
				} else {
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
							"Sort order must be 'asc' or 'desc' but it was '" + order + "'");
				}

				fields.add(new SortField(key, Type.STRING, reverse));
			}
			return new Sort(fields.toArray(new SortField[0]));
		}
	}

	/**
	 * Parses a Lucene ScoreDoc to be "searched after" from a String representation
	 * of a JSON array.
	 * 
	 * @param searchAfter String representation of a JSON object containing the
	 *                    document id or "doc" (String), score ("float") in that
	 *                    order.
	 * @return FieldDoc object built from the provided String, or null if
	 *         searchAfter was itself null or an empty String.
	 */
	private FieldDoc parseSearchAfter(String searchAfter) {
		if (searchAfter != null && !searchAfter.equals("")) {
			logger.debug("Attempting to parseSearchAfter from {}", searchAfter);
			JsonReader reader = Json.createReader(new StringReader(searchAfter));
			JsonObject object = reader.readObject();
			int doc = object.getInt("doc");
			int shardIndex = object.getInt("shardIndex");
			float score = Float.NaN;
			List<BytesRef> fields = new ArrayList<>();
			if (object.containsKey("score")) {
				score = object.getJsonNumber("score").bigDecimalValue().floatValue();
			}
			if (object.containsKey("fields")) {
				List<JsonString> jsonStrings = object.getJsonArray("fields").getValuesAs(JsonString.class);
				for (JsonString jsonString : jsonStrings) {
					fields.add(new BytesRef(jsonString.getString()));
				}
			}
			return new FieldDoc(doc, score, fields.toArray(), shardIndex);
		}
		return null;
	}

	@POST
	@Path("unlock/{entityName}")
	public void unlock(@PathParam("entityName") String entityName) throws LuceneException {
		logger.debug("Requesting unlock of {} index", entityName);
		IndexBucket bucket = indexBuckets.computeIfAbsent(entityName, k -> createBucket(k));
		if (!bucket.locked.compareAndSet(true, false)) {
			throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
					"Lucene is not currently locked for " + entityName);
		}
		try {
			int cached = bucket.indexWriter.numRamDocs();
			bucket.indexWriter.commit();
			if (cached != 0) {
				logger.debug("Unlock has committed {} {} changes to Lucene - now have {} documents indexed", cached,
						entityName, bucket.indexWriter.getDocStats().numDocs);
			}
			bucket.searcherManager.maybeRefreshBlocking();
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

}
