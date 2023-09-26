package org.icatproject.lucene;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.ejb.Singleton;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonException;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;
import jakarta.json.JsonStructure;
import jakarta.json.stream.JsonGenerator;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.DoubleRangeFacetCounts;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.LongRangeFacetCounts;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TimeLimitingCollector.TimeExceededException;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.NumericUtils;
import org.icatproject.lucene.SearchBucket.SearchType;
import org.icatproject.lucene.exceptions.LuceneException;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.IcatUnits;
import org.icatproject.utils.IcatUnits.SystemValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

@Path("/")
@Singleton
public class Lucene {

	/**
	 * A bucket for accessing the read and write functionality for a single "shard"
	 * Lucene index which can then be grouped to represent a single document type.
	 */
	private class ShardBucket {
		private FSDirectory directory;
		private IndexWriter indexWriter;
		private SearcherManager searcherManager;
		private DefaultSortedSetDocValuesReaderState state;
		private AtomicLong documentCount;

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
				indexWriter.addDocument(facetsConfig.build(doc));
				indexWriter.commit();
				indexWriter.deleteDocuments(new Term("dummy", "dummy"));
				indexWriter.commit();
				logger.debug("Now have " + indexWriter.getDocStats().numDocs + " documents indexed");
			}
			searcherManager = new SearcherManager(indexWriter, null);
			IndexSearcher indexSearcher = searcherManager.acquire();
			int numDocs = indexSearcher.getIndexReader().numDocs();
			documentCount = new AtomicLong(numDocs);
			initState(indexSearcher);
			logger.info("Created ShardBucket for directory {} with {} Documents", directory.getDirectory(), numDocs);
		}

		/**
		 * Commits all pending cached documents to this shard.
		 * 
		 * @return The number of documents committed to this shard.
		 * @throws IOException
		 */
		public int commit() throws IOException {
			if (indexWriter.hasUncommittedChanges()) {
				indexWriter.commit();
				searcherManager.maybeRefreshBlocking();
				initState(searcherManager.acquire());
			}
			return indexWriter.numRamDocs();
		}

		/**
		 * Creates a new DefaultSortedSetDocValuesReaderState object for this shard.
		 * This can be expensive for indices with a large number of faceted dimensions
		 * and labels, so should only be done when needed.
		 * 
		 * @param indexSearcher The underlying reader of this searcher is used to build
		 *                      the state
		 * @throws IOException
		 */
		private void initState(IndexSearcher indexSearcher) throws IOException {
			try {
				state = new DefaultSortedSetDocValuesReaderState(indexSearcher.getIndexReader());
			} catch (IllegalArgumentException e) {
				// This can occur if no fields in the index have been faceted, in which case set
				// state to null to ensure we don't (erroneously) use the old state
				logger.error(
						"No facets found in index, resulting in error: " + e.getClass() + " " + e.getMessage());
				state = null;
			} finally {
				searcherManager.release(indexSearcher);
			}
		}
	}

	/**
	 * A bucket for accessing the high level functionality, such as
	 * searching, for a single document type. Incoming documents will be routed to
	 * one of the individual "shard" indices that are grouped by this Object.
	 */
	private class IndexBucket {
		private String entityName;
		private List<ShardBucket> shardList = new ArrayList<>();
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
				logger.trace("Initialising bucket for {}", entityName);
				this.entityName = entityName.toLowerCase();
				Long shardIndex = 0L;
				java.nio.file.Path shardPath = luceneDirectory.resolve(entityName);
				ShardBucket shardBucket;
				// Create at least one shard, then keep creating them so long as directories
				// exist and already contain Documents
				do {
					shardBucket = new ShardBucket(shardPath);
					shardList.add(shardBucket);
					shardIndex++;
					shardPath = luceneDirectory.resolve(entityName + "_" + shardIndex);
				} while (shardBucket.documentCount.get() > 0 && Files.isDirectory(shardPath));
				logger.debug("Bucket for {} is now ready with {} shards", entityName, shardIndex);
			} catch (Throwable e) {
				logger.error("Can't continue " + e.getClass() + " " + e.getMessage());
			}
		}

		/**
		 * Acquires IndexSearchers from the SearcherManagers of the individual shards in
		 * this bucket.
		 * 
		 * @return List of IndexSearchers for all shards in this bucket.
		 * @throws IOException
		 */
		public List<IndexSearcher> acquireSearchers() throws IOException {
			List<IndexSearcher> subSearchers = new ArrayList<>();
			for (ShardBucket shardBucket : shardList) {
				logger.trace("Acquiring searcher for shard");
				subSearchers.add(shardBucket.searcherManager.acquire());
			}
			return subSearchers;
		}

		/**
		 * Adds a document to the appropriate shard for this index.
		 * 
		 * @param document The document to be added.
		 * @throws IOException
		 */
		public void addDocument(Document document) throws IOException {
			ShardBucket shardBucket = routeShard();
			shardBucket.indexWriter.addDocument(document);
			shardBucket.documentCount.incrementAndGet();
		}

		/**
		 * Deletes a document from the appropriate shard for this index.
		 * 
		 * @param icatId The ICAT id of the document to be deleted.
		 * @throws IOException
		 */
		public void deleteDocument(long icatId) throws IOException {
			for (ShardBucket shardBucket : shardList) {
				shardBucket.indexWriter.deleteDocuments(LongPoint.newExactQuery("id", icatId));
			}
		}

		/**
		 * Updates the document with the provided ICAT id.
		 * 
		 * @param icatId   The ICAT id of the document to be updated.
		 * @param document The document that will replace the old document.
		 * @throws IOException
		 */
		public void updateDocument(long icatId, Document document) throws IOException {
			deleteDocument(icatId);
			addDocument(document);
		}

		/**
		 * Creates a new ShardBucket and stores it in the shardMap.
		 * 
		 * @param shardKey The identifier for the new shard to be created. For
		 *                 simplicity, should an int starting at 0 and incrementing by 1
		 *                 for each new shard.
		 * @return A new ShardBucket with the provided shardKey.
		 * @throws IOException
		 */
		public ShardBucket buildShardBucket(int shardKey) throws IOException {
			ShardBucket shardBucket = new ShardBucket(luceneDirectory.resolve(entityName + "_" + shardKey));
			shardList.add(shardBucket);
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
			for (ShardBucket shardBucket : shardList) {
				int cached = shardBucket.commit();
				if (cached != 0) {
					int numDocs = shardBucket.indexWriter.getDocStats().numDocs;
					String directoryName = shardBucket.directory.getDirectory().toString();
					logger.debug("{} has committed {} {} changes to Lucene - now have {} documents indexed in {}",
							command, cached, entityName, numDocs, directoryName);
				}
			}
		}

		/**
		 * Commits and closes all "shard" indices for this bucket.
		 * 
		 * @throws IOException
		 */
		public void close() throws IOException {
			for (ShardBucket shardBucket : shardList) {
				shardBucket.searcherManager.close();
				shardBucket.indexWriter.commit();
				shardBucket.indexWriter.close();
				shardBucket.directory.close();
			}
		}

		/**
		 * @return The ShardBucket currently in use for indexing new Documents.
		 */
		public ShardBucket getCurrentShardBucket() {
			int size = shardList.size();
			return shardList.get(size - 1);
		}

		/**
		 * Provides the ShardBucket that should be used for writing the next Document.
		 * All Documents up to luceneMaxShardSize are indexed in the first shard, after
		 * that a new shard is created for the next luceneMaxShardSize Documents and so
		 * on.
		 * 
		 * @return The ShardBucket that the relevant Document is/should be indexed in.
		 * @throws IOException
		 */
		public ShardBucket routeShard() throws IOException {
			ShardBucket shardBucket = getCurrentShardBucket();
			if (shardBucket.documentCount.get() >= luceneMaxShardSize) {
				shardBucket.indexWriter.commit();
				shardBucket = buildShardBucket(shardList.size());
			}
			return shardBucket;
		}

		/**
		 * Releases all provided searchers for the shards in this bucket.
		 * 
		 * @param subSearchers List of IndexSearcher, in shard order.
		 * @throws IOException
		 * @throws LuceneException If the number of searchers and shards isn't the same.
		 */
		public void releaseSearchers(List<IndexSearcher> subSearchers) throws IOException, LuceneException {
			if (subSearchers.size() != shardList.size()) {
				throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR,
						"Was expecting the same number of DirectoryReaders as ShardBuckets, but had "
								+ subSearchers.size() + ", " + shardList.size() + " respectively.");
			}
			int i = 0;
			for (ShardBucket shardBucket : shardList) {
				shardBucket.searcherManager.release(subSearchers.get(i));
				i++;
			}
		}
	}

	static final Logger logger = LoggerFactory.getLogger(Lucene.class);
	private static final Marker fatal = MarkerFactory.getMarker("FATAL");
	private static final IcatAnalyzer analyzer = new IcatAnalyzer();

	private final FacetsConfig facetsConfig = new FacetsConfig();

	private java.nio.file.Path luceneDirectory;
	private int luceneCommitMillis;
	private long luceneMaxShardSize;
	private long maxSearchTimeSeconds;
	private boolean aggregateFiles;
	private Map<String, IndexBucket> indexBuckets = new ConcurrentHashMap<>();
	private Timer timer;

	public IcatUnits icatUnits;

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
		JsonStructure value = null;
		logger.debug("Requesting addNow of {}", entityName);
		try (JsonReader reader = Json.createReader(request.getInputStream())) {
			value = reader.read();
			documents = ((JsonArray) value).getValuesAs(JsonObject.class);
			for (JsonObject document : documents) {
				createNow(entityName, document);
			}
		} catch (JsonException e) {
			logger.error("Could not parse JSON from {}", value);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
		logger.debug("Added {} {} documents", documents.size(), entityName);
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
		indexBuckets.clear();

		try {
			Files.walk(luceneDirectory, FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder())
					.filter(f -> !luceneDirectory.equals(f)).map(java.nio.file.Path::toFile).forEach(File::delete);
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}

		initTimer();
		logger.info("clear complete - ready to go again");

	}

	/**
	 * Commits any pending documents to their respective index.
	 */
	@POST
	@Path("commit")
	public void commit() throws LuceneException {
		logger.debug("Requesting commit for {} IndexBuckets", indexBuckets.size());
		try {
			for (Entry<String, IndexBucket> entry : indexBuckets.entrySet()) {
				IndexBucket bucket = entry.getValue();
				if (!bucket.locked.get()) {
					logger.trace("{} is unlocked", entry.getKey());
					bucket.commit("Synch", entry.getKey());
				}
			}
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	/**
	 * Creates a new Lucene document, provided that the target index is not locked
	 * for another operation.
	 * 
	 * @param operationBody JsonObject containing the "_index" that the new "doc"
	 *                      should be created in.
	 * @throws NumberFormatException
	 * @throws IOException
	 * @throws LuceneException
	 */
	private void create(JsonObject operationBody) throws NumberFormatException, IOException, LuceneException {
		String entityName = operationBody.getString("_index");
		if (DocumentMapping.relationships.containsKey(entityName)) {
			updateByRelation(operationBody, false);
		}
		if (DocumentMapping.indexedEntities.contains(entityName)) {
			JsonObject documentObject = operationBody.getJsonObject("doc");
			Document document = parseDocument(documentObject);
			logger.trace("create {} {}", entityName, document);
			IndexBucket bucket = indexBuckets.computeIfAbsent(entityName.toLowerCase(), k -> new IndexBucket(k));
			if (bucket.locked.get()) {
				throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
						"Lucene locked for " + entityName);
			}
			bucket.addDocument(facetsConfig.build(document));
			// Special case for filesizes
			if (aggregateFiles && entityName.equals("Datafile")) {
				JsonNumber jsonFileSize = documentObject.getJsonNumber("fileSize");
				if (jsonFileSize != null) {
					JsonNumber datasetId = documentObject.getJsonNumber("dataset.id");
					JsonNumber investigationId = documentObject.getJsonNumber("investigation.id");
					aggregateFileSize(jsonFileSize.longValueExact(), 0, 1, datasetId, "dataset");
					aggregateFileSize(jsonFileSize.longValueExact(), 0, 1, investigationId, "investigation");
				}
			}
		}
	}

	/**
	 * Changes the fileSize on an entity by the specified amount. This is used to
	 * aggregate the individual fileSize of Datafiles up to Dataset and
	 * Investigation sizes.
	 * 
	 * @param sizeToAdd      Increases the fileSize of the entity by this much.
	 *                       Should be 0 for deletes.
	 * @param sizeToSubtract Decreases the fileSize of the entity by this much.
	 *                       Should be 0 for creates.
	 * @param deltaFileCount Changes the file count by this much.
	 * @param entityId       Icat id of entity to update as a JsonNumber.
	 * @param index          Index (entity) to update.
	 * @throws IOException
	 */
	private void aggregateFileSize(long sizeToAdd, long sizeToSubtract, long deltaFileCount, JsonNumber entityId,
			String index) throws IOException {
		if (entityId != null) {
			aggregateFileSize(sizeToAdd, sizeToSubtract, deltaFileCount, entityId.longValueExact(), index);
		}
	}

	/**
	 * Changes the fileSize on an entity by the specified amount. This is used to
	 * aggregate the individual fileSize of Datafiles up to Dataset and
	 * Investigation sizes.
	 * 
	 * @param sizeToAdd      Increases the fileSize of the entity by this much.
	 *                       Should be 0 for deletes.
	 * @param sizeToSubtract Decreases the fileSize of the entity by this much.
	 *                       Should be 0 for creates.
	 * @param deltaFileCount Changes the file count by this much.
	 * @param entityId       Icat id of entity to update as a long.
	 * @param index          Index (entity) to update.
	 * @throws IOException
	 */
	private void aggregateFileSize(long sizeToAdd, long sizeToSubtract, long deltaFileCount, long entityId,
			String index) throws IOException {
		long deltaFileSize = sizeToAdd - sizeToSubtract;
		if (deltaFileSize != 0 || deltaFileCount != 0) {
			IndexBucket indexBucket = indexBuckets.computeIfAbsent(index, k -> new IndexBucket(k));
			for (ShardBucket shardBucket : indexBucket.shardList) {
				shardBucket.commit();
				IndexSearcher searcher = shardBucket.searcherManager.acquire();
				try {
					Query idQuery = LongPoint.newExactQuery("id", entityId);
					TopDocs topDocs = searcher.search(idQuery, 1);
					if (topDocs.totalHits.value == 1) {
						int docId = topDocs.scoreDocs[0].doc;
						Document document = searcher.doc(docId);
						Set<String> prunedFields = new HashSet<>();
						List<IndexableField> fieldsToAdd = new ArrayList<>();

						incrementFileStatistic("fileSize", deltaFileSize, document, prunedFields, fieldsToAdd);
						incrementFileStatistic("fileCount", deltaFileCount, document, prunedFields, fieldsToAdd);

						Document newDocument = pruneDocument(prunedFields, document);
						fieldsToAdd.forEach(field -> newDocument.add(field));
						shardBucket.indexWriter.deleteDocuments(idQuery);
						shardBucket.indexWriter.addDocument(facetsConfig.build(newDocument));
						shardBucket.commit();
						break;
					}
				} finally {
					shardBucket.searcherManager.release(searcher);
				}
			}
		}
	}

	/**
	 * Increments a field relating to file statistics (count, size) as part of the
	 * update on a Document.
	 * 
	 * @param statisticName  Name of the field to increment, i.e. fileCount or
	 *                       fileSize.
	 * @param statisticDelta Change in the value of the named statistic.
	 * @param document       Lucene Document containing the old statistic value to
	 *                       be incremented.
	 * @param prunedFields   Set of fields which need to be removed from the old
	 *                       Document. If the statistic is incremented, this will
	 *                       have statisticName added to it.
	 * @param fieldsToAdd    List of Lucene IndexableFields to add to the new
	 *                       Document.
	 */
	private void incrementFileStatistic(String statisticName, long statisticDelta, Document document,
			Set<String> prunedFields, List<IndexableField> fieldsToAdd) {
		if (statisticDelta != 0) {
			prunedFields.add(statisticName);
			long oldValue = document.getField(statisticName).numericValue().longValue();
			long newValue = oldValue + statisticDelta;
			fieldsToAdd.add(new LongPoint(statisticName, newValue));
			fieldsToAdd.add(new StoredField(statisticName, newValue));
			fieldsToAdd.add(new NumericDocValuesField(statisticName, newValue));
		}
	}

	/**
	 * Creates a new Lucene document.
	 * 
	 * @param entityName   Name of the entity/index to create the document in.
	 * @param documentJson JsonObject representation of the document to be created.
	 * @throws NumberFormatException
	 * @throws IOException
	 * @throws LuceneException
	 */
	private void createNow(String entityName, JsonObject documentJson)
			throws NumberFormatException, IOException, LuceneException {
		Document document = parseDocument(documentJson);
		logger.trace("create {} {}", entityName, document);
		IndexBucket bucket = indexBuckets.computeIfAbsent(entityName.toLowerCase(), k -> new IndexBucket(k));
		bucket.addDocument(facetsConfig.build(document));
	}

	/**
	 * Perform search on the Datafile entity/index.
	 * 
	 * @param request     Incoming Http request containing the query as Json.
	 * @param searchAfter String of Json representing the last Lucene Document from
	 *                    a previous search.
	 * @param maxResults  The maximum number of results to include in the returned
	 *                    Json.
	 * @param sort        String of Json representing the sort criteria.
	 * @return String of Json representing the results of the search.
	 * @throws LuceneException
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("datafile")
	public String datafiles(@Context HttpServletRequest request, @QueryParam("search_after") String searchAfter,
			@QueryParam("maxResults") int maxResults, @QueryParam("sort") String sort) throws LuceneException {
		return searchEntity(request, searchAfter, maxResults, sort, SearchType.DATAFILE);
	}

	/**
	 * Perform search on the Dataset entity/index.
	 * 
	 * @param request     Incoming Http request containing the query as Json.
	 * @param searchAfter String of Json representing the last Lucene Document from
	 *                    a previous search.
	 * @param maxResults  The maximum number of results to include in the returned
	 *                    Json.
	 * @param sort        String of Json representing the sort criteria.
	 * @return String of Json representing the results of the search.
	 * @throws LuceneException
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("dataset")
	public String datasets(@Context HttpServletRequest request, @QueryParam("search_after") String searchAfter,
			@QueryParam("maxResults") int maxResults, @QueryParam("sort") String sort) throws LuceneException {
		return searchEntity(request, searchAfter, maxResults, sort, SearchType.DATASET);
	}

	/**
	 * Deletes a Lucene document, provided that the target index is not locked for
	 * another operation.
	 * 
	 * @param operationBody JsonObject containing the "_index" and the "_id" of the
	 *                      Document to be deleted.
	 * @throws LuceneException
	 * @throws IOException
	 */
	private void delete(JsonObject operationBody) throws LuceneException, IOException {
		String entityName = operationBody.getString("_index");
		if (DocumentMapping.relationships.containsKey(entityName)) {
			updateByRelation(operationBody, true);
		}
		if (DocumentMapping.indexedEntities.contains(entityName)) {
			long icatId = operationBody.getJsonNumber("_id").longValueExact();
			try {
				IndexBucket bucket = indexBuckets.computeIfAbsent(entityName.toLowerCase(), k -> new IndexBucket(k));
				if (bucket.locked.get()) {
					throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
							"Lucene locked for " + entityName);
				}
				logger.trace("delete {} {}", entityName, icatId);
				Query idQuery = LongPoint.newExactQuery("id", icatId);
				// Special case for filesizes
				if (aggregateFiles && entityName.equals("Datafile")) {
					for (ShardBucket shardBucket : bucket.shardList) {
						IndexSearcher datafileSearcher = shardBucket.searcherManager.acquire();
						try {
							TopDocs topDocs = datafileSearcher.search(idQuery, 1);
							if (topDocs.totalHits.value == 1) {
								int docId = topDocs.scoreDocs[0].doc;
								Document datasetDocument = datafileSearcher.doc(docId);
								long sizeToSubtract = datasetDocument.getField("fileSize").numericValue().longValue();
								if (sizeToSubtract > 0) {
									long datasetId = datasetDocument.getField("dataset.id").numericValue().longValue();
									long investigationId = datasetDocument.getField("investigation.id").numericValue()
											.longValue();
									aggregateFileSize(0, sizeToSubtract, -1, datasetId, "dataset");
									aggregateFileSize(0, sizeToSubtract, -1, investigationId, "investigation");
								}
								break;
							}
						} finally {
							shardBucket.searcherManager.release(datafileSearcher);
						}
					}
				}
				for (ShardBucket shardBucket : bucket.shardList) {
					shardBucket.indexWriter.deleteDocuments(idQuery);
				}
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
	 * @throws LuceneException
	 */
	private void encodeResult(String entityName, JsonGenerator gen, ScoreDoc hit, IndexSearcher searcher,
			SearchBucket search)
			throws IOException, LuceneException {
		int luceneDocId = hit.doc;
		int shardIndex = hit.shardIndex;
		Document document = searcher.doc(luceneDocId);
		gen.writeStartObject().write("_id", luceneDocId).write("_shardIndex", shardIndex);
		Float score = hit.score;
		if (!score.equals(Float.NaN)) {
			gen.write("_score", hit.score);
		}
		gen.writeStartObject("_source");
		document.forEach(encodeField(gen, search.fields));
		for (String joinedEntityName : search.joinedFields.keySet()) {
			List<IndexSearcher> searchers = getSearchers(search.searcherMap, joinedEntityName);
			List<ShardBucket> shards = getShards(joinedEntityName);
			SearchBucket joinedSearch = new SearchBucket(this);
			String fld;
			long parentId;
			if (joinedEntityName.toLowerCase().contains("investigation")) {
				fld = "investigation.id";
				if (entityName.equalsIgnoreCase("investigation")) {
					parentId = document.getField("id").numericValue().longValue();
				} else {
					parentId = document.getField("investigation.id").numericValue().longValue();
				}
			} else {
				fld = entityName.toLowerCase() + ".id";
				parentId = document.getField("id").numericValue().longValue();
			}
			joinedSearch.query = LongPoint.newExactQuery(fld, parentId);
			joinedSearch.sort = new Sort(new SortedNumericSortField("id", Type.LONG));
			TopFieldDocs topFieldDocs = searchShards(joinedSearch, 100, shards);
			gen.writeStartArray(joinedEntityName.toLowerCase());
			for (ScoreDoc joinedHit : topFieldDocs.scoreDocs) {
				gen.writeStartObject();
				Document joinedDocument = searchers.get(joinedHit.shardIndex).doc(joinedHit.doc);
				joinedDocument.forEach(encodeField(gen, search.joinedFields.get(joinedEntityName)));
				gen.writeEnd();
			}
			gen.writeEnd();
		}
		gen.writeEnd().writeEnd(); // source object, result object
	}

	private Consumer<? super IndexableField> encodeField(JsonGenerator gen, Set<String> fields) {
		return (field) -> {
			String fieldName = field.name();
			if (fields.contains(fieldName)) {
				if (DocumentMapping.longFields.contains(fieldName)) {
					gen.write(fieldName, field.numericValue().longValue());
				} else if (DocumentMapping.doubleFields.contains(fieldName)) {
					gen.write(fieldName, field.numericValue().doubleValue());
				} else {
					gen.write(fieldName, field.stringValue());
				}
			}
		};
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

	/**
	 * Perform faceting on an entity/index. The query associated with the request
	 * should determine which Documents to consider, and optionally the dimensions
	 * to facet. If no dimensions are provided, "sparse" faceting is performed
	 * across relevant string fields (but no Range faceting occurs).
	 * 
	 * @param entityName  Name of the entity/index to facet on.
	 * @param request     Incoming Http request containing the query as Json.
	 * @param searchAfter String of Json representing the last Lucene Document from
	 *                    a previous search.
	 * @param maxResults  The maximum number of results to include in the returned
	 *                    Json.
	 * @param maxLabels   The maximum number of labels to return for each dimension
	 *                    of the facets.
	 * @param sort        String of Json representing the sort criteria.
	 * @return String of Json representing the results of the faceting.
	 * @throws LuceneException
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{entityName}/facet")
	public String facet(@PathParam("entityName") String entityName, @Context HttpServletRequest request,
			@QueryParam("search_after") String searchAfter, @QueryParam("maxResults") int maxResults,
			@QueryParam("maxLabels") int maxLabels, @QueryParam("sort") String sort) throws LuceneException {
		SearchBucket search = null;
		try {
			search = new SearchBucket(this, SearchType.GENERIC, request, sort, null);
			return luceneFacetResult(entityName, search, searchAfter, maxResults, maxLabels);
		} catch (IOException | QueryNodeException e) {
			logger.error("Error", e);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		} finally {
			freeSearcher(search);
		}
	}

	/**
	 * Releases all IndexSearchers associated with a SearchBucket.
	 * 
	 * @param search SearchBucket to be freed.
	 * @throws LuceneException
	 */
	public void freeSearcher(SearchBucket search) throws LuceneException {
		if (search != null) {
			for (Entry<String, List<IndexSearcher>> entry : search.searcherMap.entrySet()) {
				String name = entry.getKey();
				List<IndexSearcher> subReaders = entry.getValue();
				try {
					indexBuckets.computeIfAbsent(name.toLowerCase(), k -> new IndexBucket(k))
							.releaseSearchers(subReaders);
				} catch (IOException e) {
					throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
				}
			}
		}
	}

	/**
	 * Gets all IndexSearchers needed for the shards of a given entity/index.
	 * 
	 * @param searcherMap Map of entity names to their IndexSearchers.
	 * @param name        Name of the entity to get the IndexSearchers for.
	 * @return List of IndexSearchers for name.
	 * @throws IOException
	 */
	private List<IndexSearcher> getSearchers(Map<String, List<IndexSearcher>> searcherMap, String name)
			throws IOException {
		String nameLowercase = name.toLowerCase();
		logger.trace("Get searchers for {}", nameLowercase);
		List<IndexSearcher> subSearchers = searcherMap.get(nameLowercase);
		if (subSearchers == null) {
			logger.trace("No searchers found for {}", nameLowercase);
			subSearchers = indexBuckets.computeIfAbsent(nameLowercase, k -> new IndexBucket(k)).acquireSearchers();
			searcherMap.put(nameLowercase, subSearchers);
			logger.debug("Remember searcher for {}", nameLowercase);
		}
		return subSearchers;
	}

	/**
	 * Gets a single IndexSearcher for name. When multiple shards are possible,
	 * getSearchers should be used instead.
	 * 
	 * @param searcherMap Map of entity names to their IndexSearchers.
	 * @param name        Name of the entity to get the IndexSearcher for.
	 * @return The IndexSearcher for name.
	 * @throws IOException
	 * @throws LuceneException If there are more than one shard for name.
	 */
	public IndexSearcher getSearcher(Map<String, List<IndexSearcher>> searcherMap, String name)
			throws IOException, LuceneException {
		List<IndexSearcher> subSearchers = searcherMap.get(name);
		subSearchers = getSearchers(searcherMap, name);
		if (subSearchers.size() != 1) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR,
					"Cannot get single IndexSearcher for " + name + " as it has " + subSearchers.size() + " shards");
		}
		return subSearchers.get(0);
	}

	/**
	 * Gets all ShardBuckets of a given entity/index.
	 * 
	 * @param name Name of the entity to get the ShardBuckets for.
	 * @return List of ShardBuckets for name.
	 */
	private List<ShardBucket> getShards(String name) {
		return indexBuckets.computeIfAbsent(name.toLowerCase(), k -> new IndexBucket(k)).shardList;
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
			luceneMaxShardSize = Math.max(props.getPositiveLong("maxShardSize"), Long.valueOf(Integer.MAX_VALUE + 1));
			maxSearchTimeSeconds = props.has("maxSearchTimeSeconds") ? props.getPositiveLong("maxSearchTimeSeconds")
					: 5;
			aggregateFiles = props.getBoolean("aggregateFiles", false);

			initTimer();

			icatUnits = new IcatUnits(props.getString("units", ""));

		} catch (Exception e) {
			logger.error(fatal, e.getMessage());
			throw new IllegalStateException(e.getMessage());
		}

		String format = "Initialised icat.lucene with directory {}, commitSeconds {}, maxShardSize {}, "
				+ "maxSearchTimeSeconds {}, aggregateFiles {}";
		logger.info(format, luceneDirectory, luceneCommitMillis, luceneMaxShardSize, maxSearchTimeSeconds,
				aggregateFiles);
	}

	/**
	 * Starts a timer and schedules regular commits of the IndexWriter.
	 */
	private void initTimer() {
		timer = new Timer("LuceneCommitTimer");
		timer.schedule(new CommitTimerTask(), luceneCommitMillis, luceneCommitMillis);
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

	/**
	 * Perform search on the Investigation entity/index.
	 * 
	 * @param request     Incoming Http request containing the query as Json.
	 * @param searchAfter String of Json representing the last Lucene Document from
	 *                    a previous search.
	 * @param maxResults  The maximum number of results to include in the returned
	 *                    Json.
	 * @param sort        String of Json representing the sort criteria.
	 * @return String of Json representing the results of the search.
	 * @throws LuceneException
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("investigation")
	public String investigations(@Context HttpServletRequest request, @QueryParam("search_after") String searchAfter,
			@QueryParam("maxResults") int maxResults, @QueryParam("sort") String sort) throws LuceneException {
		return searchEntity(request, searchAfter, maxResults, sort, SearchType.INVESTIGATION);
	}

	/**
	 * Locks the specified index for population, optionally removing all existing
	 * documents and preventing normal modify operations until the index is
	 * unlocked.
	 * 
	 * A check is also performed against the minId and maxId used for population.
	 * This ensures that no data is duplicated in the index.
	 * 
	 * @param entityName Name of the entity/index to lock.
	 * @param minId      The exclusive minimum ICAT id being populated for. If
	 *                   Documents already exist with an id greater than this, the
	 *                   lock will fail. If null, treated as if it were
	 *                   Long.MIN_VALUE
	 * @param maxId      The inclusive maximum ICAT id being populated for. If
	 *                   Documents already exist with an id less than or equal to
	 *                   this, the lock will fail. If null, treated as if it were
	 *                   Long.MAX_VALUE
	 * @param delete     Whether to delete all existing Documents on the index.
	 * @throws LuceneException If already locked, if there's an IOException when
	 *                         deleting documents, or if the min/max id values are
	 *                         provided and Documents already exist in that range.
	 */
	@POST
	@Path("lock/{entityName}")
	public void lock(@PathParam("entityName") String entityName, @QueryParam("minId") Long minId,
			@QueryParam("maxId") Long maxId, @QueryParam("delete") boolean delete) throws LuceneException {
		try {
			logger.info("Requesting lock of {} index, minId={}, maxId={}, delete={}", entityName, minId, maxId, delete);
			IndexBucket bucket = indexBuckets.computeIfAbsent(entityName.toLowerCase(), k -> new IndexBucket(k));

			if (!bucket.locked.compareAndSet(false, true)) {
				String message = "Lucene already locked for " + entityName;
				throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE, message);
			}
			if (delete) {
				for (ShardBucket shardBucket : bucket.shardList) {
					shardBucket.indexWriter.deleteAll();
				}
				// Reset the shardList so we reset the routing
				ShardBucket shardBucket = bucket.shardList.get(0);
				bucket.shardList = new ArrayList<>();
				bucket.shardList.add(shardBucket);
				return;
			}

			for (ShardBucket shardBucket : bucket.shardList) {
				IndexSearcher searcher = shardBucket.searcherManager.acquire();
				try {
					Query query;
					if (minId == null && maxId == null) {
						query = new MatchAllDocsQuery();
					} else {
						if (minId == null) {
							minId = Long.MIN_VALUE;
						}
						if (maxId == null) {
							maxId = Long.MAX_VALUE;
						}
						query = LongPoint.newRangeQuery("id", minId + 1, maxId);
					}
					TopDocs topDoc = searcher.search(query, 1);
					if (topDoc.scoreDocs.length != 0) {
						// If we have any results in the populating range, unlock and throw
						bucket.locked.compareAndSet(true, false);
						Document doc = searcher.doc(topDoc.scoreDocs[0].doc);
						long id = doc.getField("id").numericValue().longValue();
						String message = "While locking index, found id " + id + " in specified range";
						logger.error(message);
						throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST, message);
					}
				} finally {
					shardBucket.searcherManager.release(searcher);
				}
			}
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	/**
	 * Perform faceting on an entity/index.
	 * 
	 * @param name        Entity/index to facet.
	 * @param search      SearchBucket containing the search query, dimensions to
	 *                    facet etc.
	 * @param searchAfter String of Json representing the last Lucene Document from
	 *                    a previous search.
	 * @param maxResults  The maximum number of results from the search.
	 * @param maxLabels   The maximum number of labels to return for each dimension
	 *                    of the facets.
	 * @return String of Json representing the facets of the search results.
	 * @throws IOException
	 * @throws IllegalStateException If the IndexSearcher and its DirectoryReader
	 *                               are not in sync.
	 * @throws LuceneException       If ranges are provided for a non-numeric field,
	 *                               or something else goes wrong.
	 */
	private String luceneFacetResult(String name, SearchBucket search, String searchAfter, int maxResults,
			int maxLabels) throws IOException, IllegalStateException, LuceneException {
		// If no dimensions were specified, perform "sparse" faceting on all applicable
		// string values
		boolean sparse = search.dimensions.size() == 0;
		// By default, assume we do not need to perform string based faceting for
		// specific dimensions
		boolean facetStrings = false;
		if (maxResults <= 0 || maxLabels <= 0) {
			// This will result in no Facets and a null pointer, so return early
			logger.warn("Cannot facet when maxResults={}, maxLabels={}, returning empty list", maxResults, maxLabels);
		} else {
			// Iterate over shards and aggregate the facets from each
			logger.debug("Faceting {} with {} after {} ", name, search.query, searchAfter);
			List<ShardBucket> shards = getShards(name);
			for (ShardBucket shard : shards) {
				FacetsCollector facetsCollector = new FacetsCollector();
				IndexSearcher indexSearcher = shard.searcherManager.acquire();
				try {
					TopDocs results = FacetsCollector.search(indexSearcher, search.query, maxResults, facetsCollector);
					logger.debug("{}", results.totalHits);
					for (FacetedDimension facetedDimension : search.dimensions.values()) {
						facetStrings = facetRanges(maxLabels, facetStrings, facetsCollector, facetedDimension);
					}
					if (shard.state == null) {
						logger.debug("State not set, this is most likely due to not having any facetable fields");
						continue;
					} else if (shard.state.reader != indexSearcher.getIndexReader()) {
						logger.warn("Attempted search with outdated state, create new one from current IndexReader");
						shard.state = new DefaultSortedSetDocValuesReaderState(indexSearcher.getIndexReader());
					}
					facetStrings(search, maxLabels, sparse, facetStrings, indexSearcher, facetsCollector, shard.state);
				} finally {
					shard.searcherManager.release(indexSearcher);
				}
			}
		}
		// Build results
		JsonObjectBuilder aggregationsBuilder = Json.createObjectBuilder();
		search.dimensions.values().forEach(facetedDimension -> facetedDimension.buildResponse(aggregationsBuilder));
		String aggregations = Json.createObjectBuilder().add("aggregations", aggregationsBuilder).build().toString();
		logger.debug("aggregations: {}", aggregations);
		return aggregations;
	}

	/**
	 * Performs range based faceting on the provided facetedDimension, if possible.
	 * 
	 * @param maxLabels        The maximum number of labels to collect for each
	 *                         facet
	 * @param facetStrings     Whether there a String dimensions that will need
	 *                         faceting later
	 * @param facetsCollector  Lucene FacetsCollector used to count results
	 * @param facetedDimension Representation of the dimension to facet, and used to
	 *                         store the results of the faceting
	 * @return If a string dimension was encountered, returns true. Otherwise,
	 *         returns the value of facetStrings originally passed.
	 * @throws IOException
	 * @throws LuceneException
	 */
	private boolean facetRanges(int maxLabels, boolean facetStrings, FacetsCollector facetsCollector,
			FacetedDimension facetedDimension) throws IOException, LuceneException {
		if (facetedDimension.getRanges().size() > 0) {
			logger.debug("Ranges: {}", facetedDimension.getRanges().get(0).getClass().getSimpleName());
			// Perform range based facets for a numeric field
			String dimension = facetedDimension.getDimension();
			Facets facets;
			if (DocumentMapping.longFields.contains(dimension)) {
				LongRange[] ranges = facetedDimension.getRanges().toArray(new LongRange[0]);
				facets = new LongRangeFacetCounts(dimension, facetsCollector, ranges);
			} else if (DocumentMapping.doubleFields.contains(dimension)) {
				DoubleRange[] ranges = facetedDimension.getRanges().toArray(new DoubleRange[0]);
				facets = new DoubleRangeFacetCounts(dimension, facetsCollector, ranges);
			} else {
				throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
						"'ranges' specified for dimension " + dimension
								+ " but this is not a supported numeric field");
			}
			FacetResult facetResult = facets.getTopChildren(maxLabels, dimension);
			facetedDimension.addResult(facetResult);
		} else {
			// Have a specific string dimension to facet, but these should all be done at
			// once for efficiency
			facetStrings = true;
		}
		return facetStrings;
	}

	/**
	 * Performs String based faceting. Either this will be sparse (all fields
	 * targetted) or it will occur for specifc fields only.
	 * 
	 * @param search          Bucket being used for this search
	 * @param maxLabels       The maximum number of labels to collect for each facet
	 * @param sparse          Whether to perform sparse faceting (faceting across
	 *                        all String fields)
	 * @param facetStrings    Whether specific String dimensions should be faceted
	 * @param indexSearcher   Lucene IndexSearcher used to generate the ReaderState
	 * @param facetsCollector Lucene FacetsCollector used to count results
	 * @param state           Lucene State used to count results
	 * @throws IOException
	 */
	private void facetStrings(SearchBucket search, int maxLabels, boolean sparse, boolean facetStrings,
			IndexSearcher indexSearcher, FacetsCollector facetsCollector, DefaultSortedSetDocValuesReaderState state)
			throws IOException {
		try {
			logger.trace("String faceting");
			Facets facets = new SortedSetDocValuesFacetCounts(state, facetsCollector);
			if (sparse) {
				// Facet all applicable string fields
				addFacetResults(maxLabels, search.dimensions, facets);
				logger.trace("Sparse string faceting found results for {} dimensions", search.dimensions.size());
			} else if (facetStrings) {
				// Only add facets to the results if they match one of the requested dimensions
				List<FacetResult> facetResults = facets.getAllDims(maxLabels);
				for (FacetResult facetResult : facetResults) {
					String dimension = facetResult.dim.replace(".keyword", "");
					FacetedDimension facetedDimension = search.dimensions.get(dimension);
					logger.trace("String facets found for {}, requested dimensions were {}", dimension,
							search.dimensions.keySet());
					if (facetedDimension != null) {
						facetedDimension.addResult(facetResult);
					}
				}
			}
		} catch (IllegalArgumentException e) {
			// This can occur if no fields in the index have been faceted
			logger.error(
					"No facets found in index, resulting in error: " + e.getClass() + " " + e.getMessage());
		} catch (IllegalStateException e) {
			// This can occur if we do not create the IndexSearcher from the same
			// DirectoryReader as we used to create the state
			logger.error("IndexSearcher used is not based on the DirectoryReader used for facet counting: "
					+ e.getClass() + " " + e.getMessage());
			throw e;
		}
	}

	/**
	 * Add Facets for all dimensions. This will create FacetDimension Objects if the
	 * do not already exist in the facetedDimensionMap, otherwise the counts for
	 * each label will be aggregated.
	 * 
	 * @param maxLabels           The maximum number of labels for a given
	 *                            dimension. This labels with the highest counts are
	 *                            returned first.
	 * @param facetedDimensionMap Map containing the dimensions that have been or
	 *                            should be faceted.
	 * @param facets              Lucene facets object containing all dimensions.
	 * @throws IOException
	 */
	private void addFacetResults(int maxLabels, Map<String, FacetedDimension> facetedDimensionMap, Facets facets)
			throws IOException {
		for (FacetResult facetResult : facets.getAllDims(maxLabels)) {
			String dim = facetResult.dim.replace(".keyword", "");
			logger.trace("Sparse faceting: FacetResult for {}", dim);
			FacetedDimension facetedDimension = facetedDimensionMap.get(dim);
			if (facetedDimension == null) {
				facetedDimension = new FacetedDimension(dim);
				facetedDimensionMap.put(dim, facetedDimension);
			}
			facetedDimension.addResult(facetResult);
		}
	}

	/**
	 * Perform search on the specified entity/index.
	 * 
	 * @param request     Incoming Http request containing the query as Json.
	 * @param searchAfter String of Json representing the last Lucene Document from
	 *                    a previous search.
	 * @param maxResults  The maximum number of results to include in the returned
	 *                    Json.
	 * @param sort        String of Json representing the sort criteria.
	 * @param searchType  The type of search query to build, corresponding to one of
	 *                    the main entities.
	 * @return String of Json representing the results of the search.
	 * @throws LuceneException
	 */
	private String searchEntity(HttpServletRequest request, String searchAfter, int maxResults, String sort,
			SearchType searchType) throws LuceneException {
		SearchBucket search = null;
		try {
			search = new SearchBucket(this, searchType, request, sort, searchAfter);
			return luceneSearchResult(searchType.toString(), search, searchAfter, maxResults);
		} catch (IOException | QueryNodeException e) {
			logger.error("Error", e);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		} finally {
			freeSearcher(search);
		}
	}

	/**
	 * Perform search on name.
	 * 
	 * @param name        Entity/index to search.
	 * @param search      SearchBucket containing the search query, dimensions to
	 *                    facet etc.
	 * @param searchAfter String of Json representing the last Lucene Document from
	 *                    a previous search.
	 * @param maxResults  The maximum number of results from the search.
	 * @return String of Json representing the results of the search.
	 * @throws IOException
	 * @throws LuceneException
	 */
	private String luceneSearchResult(String name, SearchBucket search, String searchAfter, int maxResults)
			throws IOException, LuceneException {
		List<IndexSearcher> searchers = getSearchers(search.searcherMap, name);
		List<ShardBucket> shards = getShards(name);
		String format = "Search {} with: query {}, maxResults {}, searchAfter {}, scored {}, fields {}";
		logger.debug(format, name, search.query, maxResults, searchAfter, search.scored, search.fields);
		TopFieldDocs topFieldDocs = searchShards(search, maxResults, shards);
		ScoreDoc[] hits = topFieldDocs.scoreDocs;
		TotalHits totalHits = topFieldDocs.totalHits;
		SortField[] fields = topFieldDocs.fields;
		Float maxScore = Float.NaN;
		if (hits.length > 0) {
			maxScore = hits[0].score;
		}
		logger.debug("{} maxscore {}", totalHits, maxScore);
		return encodeResults(name, search, maxResults, searchers, hits, fields);
	}

	/**
	 * Performs a search by iterating over all relevant shards.
	 * 
	 * @param search     SearchBucket containing the search query, dimensions to
	 *                   facet etc.
	 * @param maxResults The maximum number of results from the search.
	 * @param shards     List of all ShardBuckets for the entity to be searched.
	 * @return Lucene TopFieldDocs resulting from the search.
	 * @throws IOException
	 * @throws LuceneException If the search runs for longer than the allowed time
	 */
	private TopFieldDocs searchShards(SearchBucket search, int maxResults, List<ShardBucket> shards)
			throws IOException, LuceneException {

		TopFieldDocs topFieldDocs;
		Counter clock = TimeLimitingCollector.getGlobalCounter();
		TimeLimitingCollector collector = new TimeLimitingCollector(null, clock, maxSearchTimeSeconds * 1000);

		try {
			List<TopFieldDocs> shardHits = new ArrayList<>();
			int doc = search.searchAfter != null ? search.searchAfter.doc : -1;
			for (ShardBucket shard : shards) {
				// Handle the possibility of some shards having a higher docCount than the doc
				// id on searchAfter
				int docCount = shard.documentCount.intValue();
				if (search.searchAfter != null) {
					if (doc > docCount) {
						search.searchAfter.doc = docCount - 1;
					} else {
						search.searchAfter.doc = doc;
					}
				}

				// Wrap Collector with TimeLimitingCollector
				TopFieldCollector topFieldCollector = TopFieldCollector.create(search.sort, maxResults,
						search.searchAfter, maxResults);
				collector.setCollector(topFieldCollector);

				IndexSearcher indexSearcher = shard.searcherManager.acquire();
				try {
					indexSearcher.search(search.query, collector);
					TopFieldDocs topDocs = topFieldCollector.topDocs();
					if (search.scored) {
						TopFieldCollector.populateScores(topDocs.scoreDocs, indexSearcher, search.query);
					}
					shardHits.add(topDocs);
				} finally {
					shard.searcherManager.release(indexSearcher);
				}
			}
			topFieldDocs = TopFieldDocs.merge(search.sort, 0, maxResults, shardHits.toArray(new TopFieldDocs[0]),
					true);

			return topFieldDocs;

		} catch (TimeExceededException e) {
			String message = "Search cancelled for exceeding " + maxSearchTimeSeconds + " seconds";
			throw new LuceneException(HttpURLConnection.HTTP_GATEWAY_TIMEOUT, message);
		}
	}

	/**
	 * Encodes the results of a search into Json.
	 * 
	 * @param name       Entity/index that has been searched search
	 * @param search     SearchBucket containing the search query, dimensions to
	 *                   facet etc.
	 * @param maxResults The maximum number of results from the search
	 * @param searchers  List of IndexSearchers for the given name
	 * @param hits       Array of the scored hits from the search
	 * @param fields     SortFields that were used to sort the hits
	 * @return String of Json encoded results
	 * @throws IOException
	 * @throws LuceneException
	 */
	private String encodeResults(String name, SearchBucket search, int maxResults, List<IndexSearcher> searchers,
			ScoreDoc[] hits, SortField[] fields) throws IOException, LuceneException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int shardIndex = -1;
		try (JsonGenerator gen = Json.createGenerator(baos)) {
			gen.writeStartObject();
			gen.writeStartArray("results");
			for (ScoreDoc hit : hits) {
				shardIndex = hit.shardIndex;
				encodeResult(name, gen, hit, searchers.get(shardIndex), search);
			}
			gen.writeEnd(); // array results
			if (hits.length == maxResults) {
				ScoreDoc lastDoc = hits[hits.length - 1];
				shardIndex = lastDoc.shardIndex;
				gen.writeStartObject("search_after").write("doc", lastDoc.doc).write("shardIndex", shardIndex);
				float lastScore = lastDoc.score;
				if (!Float.isNaN(lastScore)) {
					gen.write("score", lastScore);
				}
				if (fields != null) {
					Document lastDocument = searchers.get(shardIndex).doc(lastDoc.doc);
					gen.writeStartArray("fields");
					for (SortField sortField : fields) {
						encodeSearchAfterField(gen, sortField, lastDoc, lastDocument);
					}
					gen.writeEnd(); // end "fields" array
				}
				gen.writeEnd(); // end "search_after" object
			}
			gen.writeEnd(); // end enclosing object
		} catch (ArrayIndexOutOfBoundsException e) {
			String message = "Attempting to access searcher with shardIndex " + shardIndex + ", but only have "
					+ searchers.size() + " searchers in total";
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, message);
		}
		logger.trace("Json returned {}", baos);
		return baos.toString();
	}

	/**
	 * Encodes a single SortField used in the search into the Json as to enable the
	 * ability to "search after" the last result of a previous search.
	 * 
	 * @param gen          JsonGenerator used to encode the results
	 * @param sortField    SortField used to sort the hits
	 * @param lastDoc      The final scored hit of the search
	 * @param lastDocument The full Document corresponding to the last hit of the
	 *                     search
	 * @throws LuceneException
	 */
	private void encodeSearchAfterField(JsonGenerator gen, SortField sortField, ScoreDoc lastDoc, Document lastDocument)
			throws LuceneException {
		String fieldName = sortField.getField();
		if (fieldName == null) {
			// SCORE sorting will have a null fieldName
			if (Float.isFinite(lastDoc.score)) {
				gen.write(lastDoc.score);
			}
			return;
		}
		IndexableField indexableField = lastDocument.getField(fieldName);
		if (indexableField == null) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Field " + fieldName
					+ " used for sorting was not present on the Lucene Document; all sortable fields must also be stored.");
		}
		Type type = (sortField instanceof SortedNumericSortField)
				? ((SortedNumericSortField) sortField).getNumericType()
				: sortField.getType();
		switch (type) {
			case LONG:
				if (indexableField.numericValue() != null) {
					gen.write(indexableField.numericValue().longValue());
				} else if (indexableField.stringValue() != null) {
					gen.write(Long.valueOf(indexableField.stringValue()));
				}
				break;
			case DOUBLE:
				if (indexableField.numericValue() != null) {
					gen.write(indexableField.numericValue().doubleValue());
				} else if (indexableField.stringValue() != null) {
					gen.write(Double.valueOf(indexableField.stringValue()));
				}
				break;
			case STRING:
				gen.write(indexableField.stringValue());
				break;
			default:
				throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR,
						"SortField.Type must be one of LONG, DOUBLE, STRING, but it was " + type);
		}
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
			Field field = new Field(json, key);
			field.addToDocument(document);
			convertUnits(json, document, key);
		}
		return document;
	}

	/**
	 * If key is "type.units", all relevant numeric fields are converted to SI units
	 * and added to the document.
	 * 
	 * @param json     A JsonObject representing the Document to be built
	 * @param document The new Document being built
	 * @param key      A key present in json
	 * @retrun Whether a conversion has been performed or not
	 */
	private boolean convertUnits(JsonObject json, Document document, String key) {
		// Whenever the units are set or changed, convert to SI
		if (key.equals("type.units")) {
			String unitString = json.getString("type.units");
			convertValue(document, json, unitString, "numericValue");
			convertValue(document, json, unitString, "rangeTop");
			convertValue(document, json, unitString, "rangeBottom");
			return true;
		}
		return false;
	}

	/**
	 * Attempts to convert numericFieldName from json into SI units from its
	 * recorded unitString, and then add it to the Lucene document.
	 * 
	 * @param document         Lucene Document to add the field to.
	 * @param json             JsonObject containing the field/value pairs to be
	 *                         added.
	 * @param unitString       Units of the value to be converted.
	 * @param numericFieldName Name (key) of the field to convert and add.
	 */
	private void convertValue(Document document, JsonObject json, String unitString, String numericFieldName) {
		IndexableField field = document.getField(numericFieldName);
		double value;
		if (field != null) {
			value = NumericUtils.sortableLongToDouble(field.numericValue().longValue());
		} else if (json.containsKey(numericFieldName)) {
			value = json.getJsonNumber(numericFieldName).doubleValue();
		} else {
			// If we aren't dealing with the desired numeric field don't convert
			return;
		}
		logger.trace("Attempting to convert {} {}", value, unitString);
		SystemValue systemValue = icatUnits.new SystemValue(value, unitString);
		if (systemValue.units != null) {
			document.add(new StringField("type.unitsSI", systemValue.units, Store.YES));
		}
		if (systemValue.value != null) {
			document.add(new DoublePoint(numericFieldName + "SI", systemValue.value));
			document.add(new StoredField(numericFieldName + "SI", systemValue.value));
			long sortableLong = NumericUtils.doubleToSortableLong(systemValue.value);
			document.add(new NumericDocValuesField(numericFieldName + "SI", sortableLong));
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
	private Document updateDocumentFields(JsonObject json, Document oldDocument) {
		Document newDocument = new Document();
		List<Field> fieldsSI = new ArrayList<>();
		boolean hasNewUnits = false;
		for (IndexableField field : oldDocument.getFields()) {
			String fieldName = field.name();
			if (json.containsKey(fieldName)) {
				Field jsonField = new Field(json, fieldName);
				jsonField.addToDocument(newDocument);
				hasNewUnits = hasNewUnits || convertUnits(json, newDocument, fieldName);
			} else if (fieldName.endsWith("SI")) {
				fieldsSI.add(new Field(field));
			} else {
				Field oldField = new Field(field);
				oldField.addToDocument(newDocument);
			}
		}
		if (!hasNewUnits) {
			fieldsSI.forEach((field) -> {
				field.addToDocument(newDocument);
			});
		}
		return newDocument;
	}

	/**
	 * Returns a new Lucene Document that has the same fields as were present in
	 * oldDocument, except those provided as an argument to prune.
	 * 
	 * @param fields      These fields will not
	 *                    be present in the returned Document.
	 * @param oldDocument Lucene Document to be pruned.
	 * @return Lucene Document with pruned fields.
	 */
	private Document pruneDocument(Set<String> fields, Document oldDocument) {
		Document newDocument = new Document();
		for (IndexableField field : oldDocument.getFields()) {
			if (!fields.contains(field.name())) {
				Field fieldToAdd = new Field(field);
				fieldToAdd.addToDocument(newDocument);
			}
		}
		return newDocument;
	}

	/**
	 * Unlocks the specified index after population, committing all pending
	 * documents
	 * and allowing normal modify operations again.
	 * 
	 * @param entityName Name of the entity/index to unlock.
	 * @throws LuceneException If not locked, or if there's an IOException when
	 *                         committing documents.
	 */
	@POST
	@Path("unlock/{entityName}")
	public void unlock(@PathParam("entityName") String entityName) throws LuceneException {
		logger.debug("Requesting unlock of {} index", entityName);
		IndexBucket bucket = indexBuckets.computeIfAbsent(entityName.toLowerCase(), k -> new IndexBucket(k));
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

	/**
	 * Updates an existing Lucene document, provided that the target index is not
	 * locked for another operation.
	 * 
	 * @param operationBody JsonObject containing the "_index" that the new "doc"
	 *                      should be created in.
	 * @throws LuceneException
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	private void update(JsonObject operationBody) throws LuceneException, NumberFormatException, IOException {
		String entityName = operationBody.getString("_index");
		if (DocumentMapping.relationships.containsKey(entityName)) {
			updateByRelation(operationBody, false);
		}
		if (DocumentMapping.indexedEntities.contains(entityName)) {
			long icatId = operationBody.getJsonNumber("_id").longValueExact();
			JsonObject documentObject = operationBody.getJsonObject("doc");
			Document document = parseDocument(documentObject);
			IndexBucket bucket = indexBuckets.computeIfAbsent(entityName.toLowerCase(), k -> new IndexBucket(k));
			if (bucket.locked.get()) {
				throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
						"Lucene locked for " + entityName);
			}
			// Special case for filesizes
			if (aggregateFiles && entityName.equals("Datafile")) {
				JsonNumber jsonFileSize = documentObject.getJsonNumber("fileSize");
				if (jsonFileSize != null) {
					long sizeToSubtract = 0;
					List<IndexSearcher> datafileSearchers = bucket.acquireSearchers();
					for (IndexSearcher datafileSearcher : datafileSearchers) {
						TopDocs topDocs = datafileSearcher.search(LongPoint.newExactQuery("id", icatId), 1);
						if (topDocs.totalHits.value == 1) {
							int docId = topDocs.scoreDocs[0].doc;
							Document datasetDocument = datafileSearcher.doc(docId);
							sizeToSubtract = datasetDocument.getField("fileSize").numericValue().longValue();
							long sizeToAdd = jsonFileSize.longValueExact();
							if (sizeToAdd != sizeToSubtract) {
								JsonNumber datasetId = documentObject.getJsonNumber("dataset.id");
								JsonNumber investigationId = documentObject.getJsonNumber("investigation.id");
								aggregateFileSize(sizeToAdd, sizeToSubtract, 0, datasetId, "dataset");
								aggregateFileSize(sizeToAdd, sizeToSubtract, 0, investigationId, "investigation");
							}
							break;
						}
					}
				}
			}
			logger.trace("update: {}", document);
			bucket.updateDocument(icatId, facetsConfig.build(document));
		}
	}

	/**
	 * Updates an existing Lucene document, provided that the target index is not
	 * locked
	 * for another operation. In this case, the entity being updated does not have
	 * its own index, but exists as fields on a parent. For example,
	 * InvestigationType on an Investigation.
	 * 
	 * @param operationBody JsonObject containing the "_index" that the new "doc"
	 *                      should be created in.
	 * @param delete        Whether to delete the related entity (or just update its
	 *                      values).
	 * @throws LuceneException
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	private void updateByRelation(JsonObject operationBody, boolean delete)
			throws LuceneException, NumberFormatException, IOException {
		for (DocumentMapping.ParentRelationship parentRelationship : DocumentMapping.relationships
				.get(operationBody.getString("_index"))) {
			long childId = operationBody.getJsonNumber("_id").longValueExact();
			IndexBucket bucket = indexBuckets.computeIfAbsent(parentRelationship.parentName.toLowerCase(),
					k -> new IndexBucket(k));
			if (bucket.locked.get()) {
				throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
						"Lucene locked for " + parentRelationship.parentName);
			}
			IndexSearcher searcher = getSearcher(new HashMap<>(), parentRelationship.parentName);

			int blockSize = 10000;
			Query query = LongPoint.newExactQuery(parentRelationship.joiningField, childId);
			Sort sort = new Sort(new SortField("id", Type.LONG));
			ScoreDoc[] scoreDocs = searcher.search(query, blockSize, sort).scoreDocs;
			while (scoreDocs.length != 0) {
				for (ScoreDoc scoreDoc : scoreDocs) {
					Document oldDocument = searcher.doc(scoreDoc.doc);
					long parentId = oldDocument.getField("id").numericValue().longValue();
					Document newDocument = delete ? pruneDocument(parentRelationship.fields, oldDocument)
							: updateDocumentFields(operationBody.getJsonObject("doc"), oldDocument);
					logger.trace("updateByRelation: {}", newDocument);
					bucket.updateDocument(parentId, facetsConfig.build(newDocument));
				}
				scoreDocs = searcher.searchAfter(scoreDocs[scoreDocs.length - 1], query, blockSize, sort).scoreDocs;
			}
		}
	}

}
