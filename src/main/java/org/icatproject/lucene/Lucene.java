package org.icatproject.lucene;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
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
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;
import javax.json.stream.JsonGenerator;
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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.icatproject.lucene.exceptions.LuceneException;
import org.icatproject.utils.CheckedProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

@Path("/")
@Singleton
public class Lucene {

	private class ShardBucket {
		private FSDirectory directory;
		private IndexWriter indexWriter;
		private ReaderManager readerManager;

		/**
		 * Creates a bucket for accessing the read and write functionality for a single
		 * "shard" Lucene index which can then be grouped to represent a single document
		 * type.
		 * 
		 * @param shardPath Path to the directory used as storage for this shard.
		 * @throws IOException
		 */
		public ShardBucket(java.nio.file.Path shardPath) throws IOException {
			directory = FSDirectory.open(shardPath);
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			indexWriter = new IndexWriter(directory, config);
			String[] files = directory.listAll();
			if (files.length == 1 && files[0].equals("write.lock")) {
				logger.debug("Directory only has the write.lock file so store and delete a dummy document");
				Document doc = new Document();
				doc.add(new StringField("dummy", "dummy", Store.NO));
				indexWriter.addDocument(doc);
				indexWriter.commit();
				indexWriter.deleteDocuments(new Term("dummy", "dummy"));
				indexWriter.commit();
				logger.debug("Now have " + indexWriter.getDocStats().numDocs + " documents indexed");
			}
			readerManager = new ReaderManager(indexWriter);
		}
	}

	private class IndexBucket {
		private String entityName;
		private Map<Long, ShardBucket> shardMap = new HashMap<>();
		private AtomicBoolean locked = new AtomicBoolean();

		/**
		 * Creates a bucket for accessing the high level functionality, such as
		 * searching, for a single document type. Incoming documents will be routed to
		 * one of the individual "shard" indices that are grouped by this Object.
		 * 
		 * @param entityName The name of the entity that this index contains documents
		 *                   for.
		 */
		public IndexBucket(String entityName) {
			try {
				this.entityName = entityName;
				Long shardIndex = 0L;
				java.nio.file.Path shardPath = luceneDirectory.resolve(entityName);
				do {
					ShardBucket shardBucket = new ShardBucket(shardPath);
					shardMap.put(shardIndex, shardBucket);
					shardIndex++;
					shardPath = luceneDirectory.resolve(entityName + "_" + shardIndex);
				} while (Files.isDirectory(shardPath));
				logger.debug("Bucket for {} is now ready with {} shards", entityName, shardIndex);
			} catch (Throwable e) {
				logger.error("Can't continue " + e.getClass() + " " + e.getMessage());
			}
		}

		/**
		 * Acquires DirectoryReaders from the ReaderManagers of the individual shards in
		 * this bucket.
		 * 
		 * @return Array of DirectoryReaders for all shards in this bucket.
		 * @throws IOException
		 */
		public DirectoryReader[] acquireReaders() throws IOException {
			List<DirectoryReader> subReaders = new ArrayList<>();
			for (ShardBucket shardBucket : shardMap.values()) {
				subReaders.add(shardBucket.readerManager.acquire());
			}
			return subReaders.toArray(new DirectoryReader[0]);
		}

		/**
		 * Creates a new ShardBucket and stores it in the shardMap.
		 * 
		 * @param shardKey The identifier for the new shard to be created. For
		 *                 simplicity, should a Long starting at 0 and incrementing by 1
		 *                 for each new shard.
		 * @return A new ShardBucket with the provided shardKey.
		 * @throws IOException
		 */
		public ShardBucket buildShardBucket(Long shardKey) throws IOException {
			ShardBucket shardBucket = new ShardBucket(luceneDirectory.resolve(entityName + "_" + shardKey));
			shardMap.put(shardKey, shardBucket);
			return shardBucket;
		}

		/**
		 * Commits Documents for writing on all "shard" indices for this bucket.
		 * 
		 * @param command    The high level command which called this function. Only
		 *                   used for debug logging.
		 * @param entityName The name of the entities being committed. Only used for
		 *                   debug logging.
		 * @throws IOException
		 */
		public void commit(String command, String entityName) throws IOException {
			for (Entry<Long, ShardBucket> entry : shardMap.entrySet()) {
				ShardBucket shardBucket = entry.getValue();
				int cached = shardBucket.indexWriter.numRamDocs();
				shardBucket.indexWriter.commit();
				if (cached != 0) {
					logger.debug("{} has committed {} {} changes to Lucene - now have {} documents indexed in shard {}",
							command, cached, entityName, shardBucket.indexWriter.getDocStats().numDocs, entry.getKey());
				}
				shardBucket.readerManager.maybeRefreshBlocking();
			}
		}

		/**
		 * Commits and closes all "shard" indices for this bucket.
		 * 
		 * @throws IOException
		 */
		public void close() throws IOException {
			for (ShardBucket shardBucket : shardMap.values()) {
				shardBucket.readerManager.close();
				shardBucket.indexWriter.commit();
				shardBucket.indexWriter.close();
				shardBucket.directory.close();
			}
		}

		/**
		 * Provides the ShardBucket that should be used for reading/writing the Document
		 * with the provided id. All ids up to luceneMaxShardSize are indexed in the
		 * first shard, after that a new shard is created for the next
		 * luceneMaxShardSize Documents and so on.
		 * 
		 * @param id The id of a Document to be routed.
		 * @return The ShardBucket that the relevant Document is/should be indexed in.
		 * @throws IOException
		 */
		public ShardBucket routeShard(Long id) throws IOException {
			if (id == null) {
				// If we don't have id, provide the first bucket
				return shardMap.get(0L);
			}
			Long shard = id / luceneMaxShardSize;
			ShardBucket shardBucket = shardMap.get(shard);
			if (shardBucket == null) {
				shardBucket = buildShardBucket(shard);
			}
			return shardBucket;
		}

		/**
		 * Provides the IndexWriter that should be used for writing the Document with
		 * the provided id.
		 * 
		 * @param id The id of a Document to be routed.
		 * @return The relevant IndexWriter.
		 * @throws IOException
		 */
		public IndexWriter getWriter(Long id) throws IOException {
			return routeShard(id).indexWriter;
		}

		public void releaseReaders(DirectoryReader[] subReaders) throws IOException, LuceneException {
			if (subReaders.length != shardMap.size()) {
				throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR,
						"Was expecting the same number of DirectoryReaders as ShardBuckets, but had "
								+ subReaders.length + ", " + shardMap.size() + " respectively.");
			}
			int i = 0;
			for (ShardBucket shardBucket : shardMap.values()) {
				shardBucket.readerManager.release(subReaders[i]);
				i++;
			}
		}
	}

	public class Search {
		public Map<String, DirectoryReader[]> readerMap;
		public Query query;
		public Sort sort;
		public Set<String> fields = new HashSet<String>();
	}

	private static class ParentRelationship {
		public String parentName;
		public String fieldPrefix;

		public ParentRelationship(String parentName, String fieldPrefix) {
			this.parentName = parentName;
			this.fieldPrefix = fieldPrefix;
		}

	}

	private static final Logger logger = LoggerFactory.getLogger(Lucene.class);
	private static final Marker fatal = MarkerFactory.getMarker("FATAL");
	private static final SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");

	private static final Set<String> doubleFields = new HashSet<>();
	private static final Set<String> longFields = new HashSet<>();
	private static final Set<String> sortFields = new HashSet<>();
	private static final Set<String> textFields = new HashSet<>();
	private static final Set<String> indexedEntities = new HashSet<>();
	private static final Map<String, ParentRelationship[]> relationships = new HashMap<>();

	private static final IcatAnalyzer analyzer = new IcatAnalyzer();
	private static final StandardQueryParser genericParser = new StandardQueryParser();
	private static final StandardQueryParser datafileParser = new StandardQueryParser();
	private static final StandardQueryParser datasetParser = new StandardQueryParser();
	private static final StandardQueryParser investigationParser = new StandardQueryParser();
	private static final StandardQueryParser sampleParser = new StandardQueryParser();

	static {
		TimeZone tz = TimeZone.getTimeZone("GMT");
		df.setTimeZone(tz);

		doubleFields.add("numericValue");
		longFields.addAll(Arrays.asList("date", "startDate", "endDate", "dateTimeValue"));
		sortFields.addAll(Arrays.asList("datafile.id", "dataset.id", "investigation.id", "id", "date", "startDate",
				"endDate", "name"));
		textFields.addAll(Arrays.asList("name", "visitId", "description", "datafileFormat.name", "sample.name",
				"sample.type.name", "title", "summary", "facility.name", "user.fullName"));

		indexedEntities.addAll(Arrays.asList("Datafile", "Dataset", "Investigation", "DatafileParameter",
				"DatasetParameter", "InvestigationParameter", "InvestigationUser", "Sample"));

		relationships.put("User", new ParentRelationship[] { new ParentRelationship("InvestigationUser", "user") });
		relationships.put("Sample", new ParentRelationship[] { new ParentRelationship("Dataset", "sample") });
		relationships.put("SampleType", new ParentRelationship[] { new ParentRelationship("Sample", "type"),
				new ParentRelationship("Dataset", "sample.type") });
		relationships.put("InvestigationType",
				new ParentRelationship[] { new ParentRelationship("Investigation", "type") });
		relationships.put("DatasetType", new ParentRelationship[] { new ParentRelationship("Dataset", "type") });
		relationships.put("DatafileFormat",
				new ParentRelationship[] { new ParentRelationship("Datafile", "datafileFormat") });
		relationships.put("Facility", new ParentRelationship[] { new ParentRelationship("Investigation", "facility") });
		relationships.put("ParameterType",
				new ParentRelationship[] { new ParentRelationship("DatafileParameter", "type"),
						new ParentRelationship("DatasetParameter", "type"),
						new ParentRelationship("InvestigationParameter", "type") });

		genericParser.setAllowLeadingWildcard(true);
		genericParser.setAnalyzer(analyzer);

		CharSequence[] datafileFields = { "name", "description", "doi", "datafileFormat.name" };
		datafileParser.setAllowLeadingWildcard(true);
		datafileParser.setAnalyzer(analyzer);
		datafileParser.setMultiFields(datafileFields);

		CharSequence[] datasetFields = { "name", "description", "doi", "sample.name", "sample.type.name", "type.name" };
		datasetParser.setAllowLeadingWildcard(true);
		datasetParser.setAnalyzer(analyzer);
		datasetParser.setMultiFields(datasetFields);

		CharSequence[] investigationFields = { "name", "visitId", "title", "summary", "doi", "facility.name",
				"type.name" };
		investigationParser.setAllowLeadingWildcard(true);
		investigationParser.setAnalyzer(analyzer);
		investigationParser.setMultiFields(investigationFields);

		CharSequence[] sampleFields = { "sample.name", "sample.type.name" };
		sampleParser.setAllowLeadingWildcard(true);
		sampleParser.setAnalyzer(analyzer);
		sampleParser.setMultiFields(sampleFields);
	}

	private final FacetsConfig facetsConfig = new FacetsConfig();

	private java.nio.file.Path luceneDirectory;

	private int luceneCommitMillis;
	private Long luceneMaxShardSize;

	private AtomicLong bucketNum = new AtomicLong();
	private Map<String, IndexBucket> indexBuckets = new ConcurrentHashMap<>();

	private Timer timer;

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
		try (JsonReader reader = Json.createReader(request.getInputStream())) {
			List<JsonObject> operations = reader.readArray().getValuesAs(JsonObject.class);
			for (JsonObject operation : operations) {
				if (operation.size() != 1) {
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
							"Operation object should only have one key/value pair, but request had "
									+ operation.size());
				} else if (operation.containsKey("create")) {
					create(operation.getJsonObject("create"));
				} else if (operation.containsKey("update")) {
					update(operation.getJsonObject("update"));
				} else if (operation.containsKey("delete")) {
					delete(operation.getJsonObject("delete"));
				} else {
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
							"Operation key should be one of 'create', 'update', 'delete', but it was "
									+ operation.keySet());
				}
			}
			count = operations.size();
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
		logger.debug("Modified {} documents", count);

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
		List<JsonObject> documents;
		logger.debug("Requesting addNow of {}", entityName);
		try (JsonReader reader = Json.createReader(request.getInputStream())) {
			documents = reader.readArray().getValuesAs(JsonObject.class);
			for (JsonObject document : documents) {
				createNow(entityName, document);
			}
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
		logger.debug("Added {} {} documents", documents.size(), entityName);
	}

	/**
	 * Extracts values from queryJson in order to add one or more range query terms
	 * using queryBuilder.
	 * 
	 * Note that values in queryJson are expected to be precise only to the minute,
	 * and so to ensure that our range is inclusive, we add 59.999 seconds onto the
	 * upper value only.
	 * 
	 * If either upper or lower keys do not yield values then a half open range is
	 * created. If both are absent, then nothing is added to the query.
	 * 
	 * @param queryBuilder Builder for the Lucene query.
	 * @param queryJson    JsonObject representing the query parameters.
	 * @param lowerKey     Key in queryJson of the lower date value
	 * @param upperKey     Key in queryJson of the upper date value
	 * @param fields       Name of one or more fields to apply the range query to.
	 * @throws LuceneException
	 */
	private static void buildDateRanges(Builder queryBuilder, JsonObject queryJson, String lowerKey, String upperKey,
			String... fields) throws LuceneException {
		Long lower = parseDate(queryJson, lowerKey, 0);
		Long upper = parseDate(queryJson, upperKey, 59999);
		if (lower != null || upper != null) {
			lower = (lower == null) ? Long.MIN_VALUE : lower;
			upper = (upper == null) ? Long.MAX_VALUE : upper;
			for (String field : fields) {
				queryBuilder.add(LongPoint.newRangeQuery(field, lower, upper), Occur.MUST);
			}
		}
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
					bucket.commit("Synch", entry.getKey());
				}
			}
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private void create(JsonObject operationBody) throws NumberFormatException, IOException, LuceneException {
		String entityName = operationBody.getString("_index");
		if (relationships.containsKey(entityName)) {
			updateByRelation(operationBody, false);
		}
		if (indexedEntities.contains(entityName)) {
			String icatId = operationBody.getString("_id");
			Document document = parseDocument(operationBody.getJsonObject("doc"));
			logger.trace("create {} {}", entityName, document.toString());
			IndexBucket bucket = indexBuckets.computeIfAbsent(entityName, k -> new IndexBucket(k));
			if (bucket.locked.get()) {
				throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
						"Lucene locked for " + entityName);
			}
			bucket.getWriter(new Long(icatId)).addDocument(document);
		}
	}

	private void createNow(String entityName, JsonObject documentJson)
			throws NumberFormatException, IOException, LuceneException {
		if (!documentJson.containsKey("id")) {
			throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
					"id was not in the document keys " + documentJson.keySet());
		}
		String icatId = documentJson.getString("id");
		Document document = parseDocument(documentJson);
		logger.trace("create {} {}", entityName, document.toString());
		IndexBucket bucket = indexBuckets.computeIfAbsent(entityName, k -> new IndexBucket(k));
		bucket.getWriter(new Long(icatId)).addDocument(document);
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
			Search search = datafilesQuery(request, sort, uid);
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
	@Path("datafiles/facet")
	public String datafilesFacet(@Context HttpServletRequest request, @QueryParam("search_after") String searchAfter, @QueryParam("maxResults") int maxResults,
			@QueryParam("maxLabels") int maxLabels, @QueryParam("sort") String sort) throws LuceneException {
		Long uid = null;
		try {
			uid = bucketNum.getAndIncrement();
			Search search = datafilesQuery(request, sort, uid);
			return luceneFacetResult("Datafile", search, searchAfter, maxResults, maxLabels, uid);
		} catch (Exception e) {
			logger.error("Error", e);
			freeSearcher(uid);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}

	}

	private Search datafilesQuery(HttpServletRequest request, String sort, Long uid) throws IOException, QueryNodeException, LuceneException {
		Search search = new Search();
		searches.put(uid, search);
		Map<String, DirectoryReader[]> readerMap = new HashMap<>();
		search.readerMap = readerMap;
		search.sort = parseSort(sort);

		try (JsonReader r = Json.createReader(request.getInputStream())) {
			JsonObject o = r.readObject();
			JsonObject query = o.getJsonObject("query");
			String userName = query.getString("user", null);

			BooleanQuery.Builder theQuery = new BooleanQuery.Builder();

			if (userName != null) {
				Query iuQuery = JoinUtil.createJoinQuery("investigation.id", false, "investigation.id",
						new TermQuery(new Term("user.name", userName)), getSearcher(readerMap, "InvestigationUser"),
						ScoreMode.None);
				theQuery.add(iuQuery, Occur.MUST);
			}

			String text = query.getString("text", null);
			if (text != null) {
				theQuery.add(datafileParser.parse(text, null), Occur.MUST);
			}

			buildDateRanges(theQuery, query, "lower", "upper", "date");

			if (query.containsKey("parameters")) {
				JsonArray parameters = query.getJsonArray("parameters");
				IndexSearcher datafileParameterSearcher = getSearcher(readerMap, "DatafileParameter");
				for (JsonValue p : parameters) {
					BooleanQuery.Builder paramQuery = parseParameter(p);
					Query toQuery = JoinUtil.createJoinQuery("datafile.id", false, "id", paramQuery.build(),
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
		return search;
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
			Map<String, DirectoryReader[]> readerMap = new HashMap<>();
			search.readerMap = readerMap;
			search.sort = parseSort(sort);
			try (JsonReader r = Json.createReader(request.getInputStream())) {
				JsonObject o = r.readObject();
				JsonObject query = o.getJsonObject("query");
				String userName = query.getString("user", null);

				BooleanQuery.Builder theQuery = new BooleanQuery.Builder();

				if (userName != null) {

					Query iuQuery = JoinUtil.createJoinQuery("investigation.id", false, "investigation.id",
							new TermQuery(new Term("user.name", userName)), getSearcher(readerMap, "InvestigationUser"),
							ScoreMode.None);

					theQuery.add(iuQuery, Occur.MUST);
				}

				String text = query.getString("text", null);
				if (text != null) {
					theQuery.add(datasetParser.parse(text, null), Occur.MUST);
				}

				buildDateRanges(theQuery, query, "lower", "upper", "startDate", "endDate");

				if (query.containsKey("parameters")) {
					JsonArray parameters = query.getJsonArray("parameters");
					IndexSearcher datasetParameterSearcher = getSearcher(readerMap, "DatasetParameter");
					for (JsonValue p : parameters) {
						BooleanQuery.Builder paramQuery = parseParameter(p);
						Query toQuery = JoinUtil.createJoinQuery("dataset.id", false, "id", paramQuery.build(),
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

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("datasets/facet")
	public String datasetsFacet(@Context HttpServletRequest request, @QueryParam("search_after") String searchAfter, @QueryParam("maxResults") int maxResults,
			@QueryParam("maxLabels") int maxLabels, @QueryParam("sort") String sort) throws LuceneException {
		Long uid = null;
		try {
			uid = bucketNum.getAndIncrement();
			Search search = datasetsQuery(request, sort, uid);
			return luceneFacetResult("Dataset", search, searchAfter, maxResults, maxLabels, uid);
		} catch (Exception e) {
			logger.error("Error", e);
			freeSearcher(uid);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}

	}

	private Search datasetsQuery(HttpServletRequest request, String sort, Long uid) throws IOException, QueryNodeException, LuceneException {
		Search search = new Search();
		searches.put(uid, search);
		Map<String, DirectoryReader[]> readerMap = new HashMap<>();
		search.readerMap = readerMap;
		search.sort = parseSort(sort);
		try (JsonReader r = Json.createReader(request.getInputStream())) {
			JsonObject o = r.readObject();
			JsonObject query = o.getJsonObject("query");
			String userName = query.getString("user", null);

			BooleanQuery.Builder theQuery = new BooleanQuery.Builder();

			if (userName != null) {

				Query iuQuery = JoinUtil.createJoinQuery("investigation.id", false, "investigation.id",
						new TermQuery(new Term("user.name", userName)), getSearcher(readerMap, "InvestigationUser"),
						ScoreMode.None);

				theQuery.add(iuQuery, Occur.MUST);
			}

			String text = query.getString("text", null);
			if (text != null) {
				theQuery.add(datasetParser.parse(text, null), Occur.MUST);
			}

			buildDateRanges(theQuery, query, "lower", "upper", "startDate", "endDate");

			if (query.containsKey("parameters")) {
				JsonArray parameters = query.getJsonArray("parameters");
				IndexSearcher datasetParameterSearcher = getSearcher(readerMap, "DatasetParameter");
				for (JsonValue p : parameters) {
					BooleanQuery.Builder paramQuery = parseParameter(p);
					Query toQuery = JoinUtil.createJoinQuery("dataset.id", false, "id", paramQuery.build(),
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
		return search;
	}

	/**
	 * Converts String into number of ms since epoch.
	 * 
	 * @param value String representing a Date in the format "yyyyMMddHHmm".
	 * @return Number of ms since epoch, or null if value was null
	 * @throws java.text.ParseException
	 */
	protected static Long decodeTime(String value) throws java.text.ParseException {
		if (value == null) {
			return null;
		} else {
			synchronized (df) {
				return df.parse(value).getTime();
			}
		}
	}

	private void delete(JsonObject operationBody) throws LuceneException, IOException {
		String entityName = operationBody.getString("_index");
		if (relationships.containsKey(entityName)) {
			updateByRelation(operationBody, true);
		}
		if (indexedEntities.contains(entityName)) {
			String icatId = operationBody.getString("_id");
			try {
				IndexBucket bucket = indexBuckets.computeIfAbsent(entityName, k -> new IndexBucket(k));
				if (bucket.locked.get()) {
					throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
							"Lucene locked for " + entityName);
				}
				logger.trace("delete {} {}", entityName, icatId);
				ShardBucket shardBucket = bucket.routeShard(new Long(icatId));
				shardBucket.indexWriter.deleteDocuments(new Term("id", icatId));
			} catch (IOException e) {
				throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
			}
		}
	}

	/**
	 * Encodes core Lucene information (keys preceded by underscores) and a
	 * selection of the Document's source fields to JSON to be returned to
	 * icat.server. Note that "_id" is the Lucene Document id, and should not be
	 * confused with the ICAT entity id, which should be denoted by the key "id"
	 * within the "_source" object.
	 * 
	 * @param gen      JsonGenerator to encode the information to.
	 * @param hit      ScoreDoc representing a single search result.
	 * @param searcher IndexSearcher used to get the Document for the hit.
	 * @param search   Search object containing the fields to return.
	 * @throws IOException
	 */
	private void encodeResult(JsonGenerator gen, ScoreDoc hit, IndexSearcher searcher, Search search)
			throws IOException {
		int luceneDocId = hit.doc;
		Document document = searcher.doc(luceneDocId);
		gen.writeStartObject().write("_id", luceneDocId);
		Float score = hit.score;
		if (!score.equals(Float.NaN)) {
			gen.write("_score", hit.score);
		}
		gen.writeStartObject("_source");
		document.forEach((field) -> {
			String fieldName = field.name();
			if (search.fields.contains(fieldName)) {
				if (longFields.contains(fieldName)) {
					gen.write(fieldName, field.numericValue().longValue());
				} else if (doubleFields.contains(fieldName)) {
					gen.write(fieldName, field.numericValue().doubleValue());
				} else {
					gen.write(fieldName, field.stringValue());
				}
			}
		});
		gen.writeEnd().writeEnd(); // source object, result object
	}

	@PreDestroy
	private void exit() {
		logger.info("Closing down icat.lucene");

		if (timer != null) {
			timer.cancel();
			timer = null; // This seems to be necessary to make it really stop
		}
		try {
			for (IndexBucket bucket : indexBuckets.values()) {
				bucket.close();
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
			Map<String, DirectoryReader[]> search = searches.get(uid).readerMap;
			for (Entry<String, DirectoryReader[]> entry : search.entrySet()) {
				String name = entry.getKey();
				DirectoryReader[] subReaders = entry.getValue();
				try {
					indexBuckets.computeIfAbsent(name, k -> new IndexBucket(k)).releaseReaders(subReaders);
				} catch (IOException e) {
					throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
				}
			}
			searches.remove(uid);
		}
	}

	private MultiReader getMultiReader(Map<String, DirectoryReader[]> readerMap, String name) throws IOException {
		DirectoryReader[] subReaders = readerMap.get(name);
		if (subReaders == null) {
			subReaders = indexBuckets.computeIfAbsent(name, k -> new IndexBucket(k)).acquireReaders();
			readerMap.put(name, subReaders);
			logger.debug("Remember searcher for {}", name);
		}
		return new MultiReader(subReaders, false);
	}

	private IndexSearcher getSearcher(Map<String, DirectoryReader[]> readerMap, String name) throws IOException {
		return new IndexSearcher(getMultiReader(readerMap, name));
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
			luceneMaxShardSize = Math.max(props.getPositiveLong("maxShardSize"), new Long(Integer.MAX_VALUE + 1));

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
			Search search = investigationsQuery(request, sort, uid);
			return luceneSearchResult("Investigation", search, searchAfter, maxResults, uid);
		} catch (Exception e) {
			logger.error("Error", e);
			freeSearcher(uid);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("investigations/facet")
	public String investigationsFacet(@Context HttpServletRequest request, @QueryParam("search_after") String searchAfter, @QueryParam("maxResults") int maxResults,
			@QueryParam("maxLabels") int maxLabels, @QueryParam("sort") String sort) throws LuceneException {
		Long uid = null;
		try {
			uid = bucketNum.getAndIncrement();
			Search search = investigationsQuery(request, sort, uid);
			return luceneFacetResult("Investigation", search, searchAfter, maxResults, maxLabels, uid);
		} catch (Exception e) {
			logger.error("Error", e);
			freeSearcher(uid);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private Search investigationsQuery(HttpServletRequest request, String sort, Long uid) throws IOException, QueryNodeException, LuceneException {
		Search search = new Search();
		searches.put(uid, search);
		Map<String, DirectoryReader[]> readerMap = new HashMap<>();
		search.readerMap = readerMap;
		search.sort = parseSort(sort);
		try (JsonReader r = Json.createReader(request.getInputStream())) {
			JsonObject o = r.readObject();
			JsonObject query = o.getJsonObject("query");
			String userName = query.getString("user", null);

			BooleanQuery.Builder theQuery = new BooleanQuery.Builder();

			if (userName != null) {
				Query iuQuery = JoinUtil.createJoinQuery("investigation.id", false, "id",
						new TermQuery(new Term("user.name", userName)), getSearcher(readerMap, "InvestigationUser"),
						ScoreMode.None);
				theQuery.add(iuQuery, Occur.MUST);
			}

			String text = query.getString("text", null);
			if (text != null) {
				theQuery.add(investigationParser.parse(text, null), Occur.MUST);
			}

			buildDateRanges(theQuery, query, "lower", "upper", "startDate", "endDate");

			if (query.containsKey("parameters")) {
				JsonArray parameters = query.getJsonArray("parameters");
				IndexSearcher investigationParameterSearcher = getSearcher(readerMap, "InvestigationParameter");

				for (JsonValue p : parameters) {
					BooleanQuery.Builder paramQuery = parseParameter(p);
					Query toQuery = JoinUtil.createJoinQuery("investigation.id", false, "id", paramQuery.build(),
							investigationParameterSearcher, ScoreMode.None);
					theQuery.add(toQuery, Occur.MUST);
				}
			}

			if (query.containsKey("samples")) {
				JsonArray samples = query.getJsonArray("samples");
				IndexSearcher sampleSearcher = getSearcher(readerMap, "Sample");

				for (JsonValue s : samples) {
					JsonString sample = (JsonString) s;
					BooleanQuery.Builder sampleQuery = new BooleanQuery.Builder();
					sampleQuery.add(sampleParser.parse(sample.getString(), null), Occur.MUST);
					Query toQuery = JoinUtil.createJoinQuery("investigation.id", false, "id", sampleQuery.build(),
							sampleSearcher, ScoreMode.None);
					theQuery.add(toQuery, Occur.MUST);
				}
			}

			String userFullName = query.getString("userFullName", null);
			if (userFullName != null) {
				BooleanQuery.Builder userFullNameQuery = new BooleanQuery.Builder();
				userFullNameQuery.add(genericParser.parse(userFullName, "user.fullName"), Occur.MUST);
				IndexSearcher investigationUserSearcher = getSearcher(readerMap, "InvestigationUser");
				Query toQuery = JoinUtil.createJoinQuery("investigation.id", false, "id", userFullNameQuery.build(),
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
		return search;
	}

	@POST
	@Path("lock/{entityName}")
	public void lock(@PathParam("entityName") String entityName) throws LuceneException {
		logger.info("Requesting lock of {} index", entityName);
		IndexBucket bucket = indexBuckets.computeIfAbsent(entityName, k -> new IndexBucket(k));

		if (!bucket.locked.compareAndSet(false, true)) {
			throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE, "Lucene already locked for " + entityName);
		}
		try {
			for (ShardBucket shardBucket : bucket.shardMap.values()) {
				shardBucket.indexWriter.deleteAll();
			}
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private String luceneFacetResult(String name, Search search, String searchAfter, int maxResults, int maxLabels, Long uid)
			throws IOException, IllegalStateException {
		List<FacetResult> results;
		if (maxResults <= 0 || maxLabels <= 0) {
			// This will result in no Facets and a null pointer, so return early
			logger.warn("No facets possible for maxResults={}, maxLabels={}, returning empty list", maxResults, maxLabels);
			results = new ArrayList<>();
		} else {
			MultiReader directoryReader = getMultiReader(search.readerMap, name);
			IndexSearcher indexSearcher = new IndexSearcher(directoryReader);
			logger.debug("To facet in {} for {} {} with {} from {} ", name, search.query, maxResults, indexSearcher,
					searchAfter);
			try {
				DefaultSortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(directoryReader);
				FacetsCollector facetsCollector = new FacetsCollector();
				FacetsCollector.search(indexSearcher, search.query, maxResults, facetsCollector);
				Facets facets = new SortedSetDocValuesFacetCounts(state, facetsCollector);
				logger.debug("facets: {}, maxLabels: {}, maxResults: {}", facets, maxLabels, maxResults);
				results = facets.getAllDims(maxLabels);
			} catch (IllegalArgumentException e) {
				// This can occur if no fields in the index have been faceted
				logger.error("No facets found in index, resulting in error: " + e.getClass() + " " + e.getMessage());
				results = new ArrayList<>();
			} catch (IllegalStateException e) {
				// This can occur if we do not create the IndexSearcher from the same DirectoryReader as we used to
				// create the state
				logger.error("IndexSearcher used is not based on the DirectoryReader used for facet counting: "
					+ e.getClass() + " " + e.getMessage());
				throw e;
			}
			logger.debug("Facets found for " + results.size() + " dimensions");
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (JsonGenerator gen = Json.createGenerator(baos)) {
			gen.writeStartObject();
			gen.writeStartObject("dimensions"); // object containing all facet dimensions
			for (FacetResult result : results) {
				gen.writeStartObject(result.dim); // object containing labelValues for a given dimension
				for (LabelAndValue labelValue : result.labelValues) {
					gen.write(labelValue.label, labelValue.value.longValue());
				}
				gen.writeEnd(); // object containing labelValues
			}
			gen.writeEnd(); // object containing dimensions
			gen.writeEnd();
		}
		logger.debug("Json returned {}", baos.toString());
		return baos.toString();
	}

	private String luceneSearchResult(String name, Search search, String searchAfter, int maxResults, Long uid)
			throws IOException, LuceneException {
		IndexSearcher isearcher = getSearcher(search.readerMap, name);
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
			gen.writeStartObject().writeStartArray("results");
			for (ScoreDoc hit : hits) {
				encodeResult(gen, hit, isearcher, search);
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
						IndexableField indexableField = lastDocument.getField(sortField.getField());
						if (indexableField == null) {
							throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Field "
									+ sortField.getField()
									+ " used for sorting was not present on the Lucene Document; all sortable fields must also be stored.");
						}
						Type type = (sortField instanceof SortedNumericSortField)
								? ((SortedNumericSortField) sortField).getNumericType()
								: sortField.getType();
						switch (type) {
							case LONG:
								gen.write(indexableField.numericValue().longValue());
								break;
							case DOUBLE:
								gen.write(indexableField.numericValue().doubleValue());
								break;
							case STRING:
								gen.write(indexableField.stringValue());
								break;
							default:
								throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR,
										"SortField.Type must be one of LONG, DOUBLE, STRING, but it was " + type);
						}
					}
					gen.writeEnd(); // end "fields" array
				}
				gen.writeEnd(); // end "search_after" object
			}
			gen.writeEnd(); // end enclosing object
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

	/**
	 * Parses a date/time value from jsonObject. Can account for either a Long
	 * value, or a String value encoded in the format yyyyMMddHHmm.
	 * 
	 * @param jsonObject JsonObject containing the date to be parsed.
	 * @param key        Key of the date/time value in jsonObject.
	 * @param offset     In the case of STRING ValueType, add offset ms before
	 *                   returning. This accounts for the fact the String format
	 *                   used is only precise to minutes and not seconds.
	 * @return null if jsonObject does not contain the key, number of ms since epoch
	 *         otherwise.
	 * @throws LuceneException If the ValueType is not NUMBER or STRING, or if a
	 *                         STRING value cannot be parsed.
	 */
	private static Long parseDate(JsonObject jsonObject, String key, int offset) throws LuceneException {
		if (jsonObject.containsKey(key)) {
			ValueType valueType = jsonObject.get(key).getValueType();
			switch (valueType) {
				case STRING:
					String dateString = jsonObject.getString(key);
					try {
						return decodeTime(dateString) + offset;
					} catch (Exception e) {
						throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
								"Could not parse date " + dateString + " using expected format yyyyMMddHHmm");
					}
				case NUMBER:
					return jsonObject.getJsonNumber(key).longValueExact();
				default:
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
							"Dates should be represented by a NUMBER or STRING JsonValue, but got " + valueType);
			}
		}
		return null;
	}

	/**
	 * Builds a Lucene Document from the parsed json.
	 * 
	 * @param json Key value pairs of fields.
	 * @return Lucene Document.
	 */
	private Document parseDocument(JsonObject json) {
		Document document = new Document();
		for (String key : json.keySet()) {
			addField(json, document, key);
		}
		return document;
	}

	private void addField(JsonObject json, Document document, String key) {
		// SortedDocValuesField need to be indexed in addition to indexing a Field for
		// searching/storing, so deal with that first
		addSortField(json, document, key);

		if (doubleFields.contains(key)) {
			Double value = json.getJsonNumber(key).doubleValue();
			document.add(new DoublePoint(key, value));
			document.add(new StoredField(key, value));
		} else if (longFields.contains(key)) {
			Long value = json.getJsonNumber(key).longValueExact();
			document.add(new LongPoint(key, value));
			document.add(new StoredField(key, value));
		} else if (textFields.contains(key)) {
			document.add(new TextField(key, json.getString(key), Store.YES));
		} else {
			document.add(new StringField(key, json.getString(key), Store.YES));
		}
	}

	private void addSortField(JsonObject json, Document document, String key) {
		if (sortFields.contains(key)) {
			if (longFields.contains(key)) {
				document.add(new SortedNumericDocValuesField(key, json.getJsonNumber(key).longValueExact()));
			} else if (doubleFields.contains(key)) {
				long sortableLong = NumericUtils.doubleToSortableLong(json.getJsonNumber(key).doubleValue());
				document.add(new SortedNumericDocValuesField(key, sortableLong));
			} else {
				document.add(new SortedDocValuesField(key, new BytesRef(json.getString(key))));
			}
		}
	}

	private void addSortField(IndexableField field, Document document) {
		String key = field.name();
		if (sortFields.contains(key)) {
			if (longFields.contains(key)) {
				document.add(new SortedNumericDocValuesField(key, field.numericValue().longValue()));
			} else if (doubleFields.contains(key)) {
				long sortableLong = NumericUtils.doubleToSortableLong(field.numericValue().doubleValue());
				document.add(new SortedNumericDocValuesField(key, sortableLong));
			} else {
				document.add(new SortedDocValuesField(key, new BytesRef(field.stringValue())));
			}
		}
	}

	/**
	 * Returns a new Lucene Document that has the same fields as were present in
	 * oldDocument, except in cases where json has an entry for that field. In this
	 * case, the json value is used instead.
	 * 
	 * @param json        Key value pairs of fields to overwrite fields already
	 *                    present in oldDocument.
	 * @param oldDocument Lucene Document to be updated.
	 * @return Lucene Document with updated fields.
	 */
	private Document updateDocument(JsonObject json, Document oldDocument) {
		Document newDocument = new Document();
		for (IndexableField field : oldDocument.getFields()) {
			String fieldName = field.name();
			if (json.keySet().contains(fieldName)) {
				addField(json, newDocument, fieldName);
			} else {
				addSortField(field, newDocument);
				newDocument.add(field);
			}
		}
		return newDocument;
	}

	/**
	 * Returns a new Lucene Document that has the same fields as were present in
	 * oldDocument, except in cases where the field name starts with fieldPrefix.
	 * 
	 * @param fieldPrefix Any fields with a name starting with this String will not
	 *                    be present in the returned Document.
	 * @param oldDocument Lucene Document to be pruned.
	 * @return Lucene Document with pruned fields.
	 */
	private Document pruneDocument(String fieldPrefix, Document oldDocument) {
		Document newDocument = new Document();
		for (IndexableField field : oldDocument.getFields()) {
			if (!field.name().startsWith(fieldPrefix)) {
				addSortField(field, newDocument);
				newDocument.add(field);
			}
		}
		return newDocument;
	}

	private Builder parseParameter(JsonValue p) throws LuceneException {
		JsonObject parameter = (JsonObject) p;
		BooleanQuery.Builder paramQuery = new BooleanQuery.Builder();
		String pName = parameter.getString("name", null);
		if (pName != null) {
			paramQuery.add(new WildcardQuery(new Term("type.name", pName)), Occur.MUST);
		}

		String pUnits = parameter.getString("units", null);
		if (pUnits != null) {
			paramQuery.add(new WildcardQuery(new Term("type.units", pUnits)), Occur.MUST);
		}
		if (parameter.containsKey("stringValue")) {
			String pStringValue = parameter.getString("stringValue", null);
			paramQuery.add(new WildcardQuery(new Term("stringValue", pStringValue)), Occur.MUST);
		} else if (parameter.containsKey("lowerDateValue") && parameter.containsKey("upperDateValue")) {
			buildDateRanges(paramQuery, parameter, "lowerDateValue", "upperDateValue", "dateTimeValue");
		} else if (parameter.containsKey("lowerNumericValue") && parameter.containsKey("upperNumericValue")) {
			Double pLowerNumericValue = parameter.getJsonNumber("lowerNumericValue").doubleValue();
			Double pUpperNumericValue = parameter.getJsonNumber("upperNumericValue").doubleValue();
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

				if (longFields.contains(key)) {
					fields.add(new SortedNumericSortField(key, Type.LONG, reverse));
				} else if (doubleFields.contains(key)) {
					fields.add(new SortedNumericSortField(key, Type.DOUBLE, reverse));
				} else {
					fields.add(new SortField(key, Type.STRING, reverse));
				}
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
	 * @throws LuceneException If an entry in the fields array is not a STRING or
	 *                         NUMBER
	 */
	private FieldDoc parseSearchAfter(String searchAfter) throws LuceneException {
		if (searchAfter != null && !searchAfter.equals("")) {
			logger.debug("Attempting to parseSearchAfter from {}", searchAfter);
			JsonReader reader = Json.createReader(new StringReader(searchAfter));
			JsonObject object = reader.readObject();
			int doc = object.getInt("doc");
			int shardIndex = object.getInt("shardIndex");
			float score = Float.NaN;
			List<Object> fields = new ArrayList<>();
			if (object.containsKey("score")) {
				score = object.getJsonNumber("score").bigDecimalValue().floatValue();
			}
			if (object.containsKey("fields")) {
				JsonArray jsonArray = object.getJsonArray("fields");
				for (JsonValue value : jsonArray) {
					switch (value.getValueType()) {
						case NUMBER:
							JsonNumber number = ((JsonNumber) value);
							if (number.toString().contains(".")) {
								fields.add(number.doubleValue());
							} else {
								fields.add(number.longValueExact());
							}
							break;
						case STRING:
							fields.add(new BytesRef(((JsonString) value).getString()));
							break;
						default:
							throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
									"fields should be an array of STRING and NUMBER, but had entry of type "
											+ value.getValueType());
					}
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
		IndexBucket bucket = indexBuckets.computeIfAbsent(entityName, k -> new IndexBucket(k));
		if (!bucket.locked.compareAndSet(true, false)) {
			throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
					"Lucene is not currently locked for " + entityName);
		}
		try {
			bucket.commit("Unlock", entityName);
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private void update(JsonObject operationBody) throws LuceneException, NumberFormatException, IOException {
		String entityName = operationBody.getString("_index");
		if (relationships.containsKey(entityName)) {
			updateByRelation(operationBody, false);
		}
		if (indexedEntities.contains(entityName)) {
			String icatId = operationBody.getString("_id");
			Document document = parseDocument(operationBody.getJsonObject("doc"));
			IndexBucket bucket = indexBuckets.computeIfAbsent(entityName, k -> new IndexBucket(k));
			if (bucket.locked.get()) {
				throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
						"Lucene locked for " + entityName);
			}
			logger.trace("update: {}", document);
			bucket.getWriter(new Long(icatId)).updateDocument(new Term("id", icatId), document);
		}
	}

	private void updateByRelation(JsonObject operationBody, Boolean delete)
			throws LuceneException, NumberFormatException, IOException {
		for (ParentRelationship parentRelationship : relationships.get(operationBody.getString("_index"))) {
			String childId = operationBody.getString("_id");
			IndexBucket bucket = indexBuckets.computeIfAbsent(parentRelationship.parentName, k -> new IndexBucket(k));
			if (bucket.locked.get()) {
				throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
						"Lucene locked for " + parentRelationship.parentName);
			}
			IndexSearcher searcher = getSearcher(new HashMap<>(), parentRelationship.parentName);

			int blockSize = 10000;
			TermQuery query = new TermQuery(new Term(parentRelationship.fieldPrefix + ".id", childId));
			Sort sort = new Sort(new SortField("id", Type.STRING));
			ScoreDoc[] scoreDocs = searcher.search(query, blockSize, sort).scoreDocs;
			while (scoreDocs.length != 0) {
				TopDocs topDocs = searcher.search(query, blockSize);
				for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
					Document oldDocument = searcher.doc(scoreDoc.doc);
					String parentId = oldDocument.get("id");
					Document newDocument = delete ? pruneDocument(parentRelationship.fieldPrefix, oldDocument)
							: updateDocument(operationBody.getJsonObject("doc"), oldDocument);
					logger.trace("updateByRelation: {}", newDocument);
					bucket.getWriter(new Long(parentId)).updateDocument(new Term("id", parentId), newDocument);
				}
				scoreDocs = searcher.searchAfter(scoreDocs[scoreDocs.length - 1], query, blockSize, sort).scoreDocs;
			}
		}
	}

}
