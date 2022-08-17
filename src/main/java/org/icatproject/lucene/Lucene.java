package org.icatproject.lucene;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
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

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.Singleton;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.stream.JsonGenerator;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
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
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
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
			IndexWriterConfig config = new IndexWriterConfig(DocumentMapping.analyzer);
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
			IndexSearcher indexSearcher = null;
			int numDocs;
			try {
				indexSearcher = searcherManager.acquire();
				numDocs = indexSearcher.getIndexReader().numDocs();
				documentCount = new AtomicLong(numDocs);
			} finally {
				searcherManager.release(indexSearcher);
			}
			logger.info("Created ShardBucket for {} with {} Documents", directory, numDocs);
		}

		/**
		 * Commits all pending cached documents to this shard.
		 * 
		 * @return The number of documents committed to this shard.
		 * @throws IOException
		 */
		public int commit() throws IOException {
			int cached = indexWriter.numRamDocs();
			indexWriter.commit();
			searcherManager.maybeRefreshBlocking();
			return cached;
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
		 * Updates documents matching the term with the provided document.
		 * 
		 * @param term     Term identifying the old document(s) to be updated.
		 * @param document The document that will replace the old document(s).
		 * @throws IOException
		 */
		public void updateDocument(Term term, Document document) throws IOException {
			for (ShardBucket shardBucket : shardList) {
				shardBucket.indexWriter.updateDocument(term, document);
			}
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

	private final FacetsConfig facetsConfig = new FacetsConfig();

	private java.nio.file.Path luceneDirectory;
	private int luceneCommitMillis;
	private Long luceneMaxShardSize;
	private long maxSearchTimeSeconds;
	private boolean aggregateFiles;

	private AtomicLong bucketNum = new AtomicLong();
	private Map<String, IndexBucket> indexBuckets = new ConcurrentHashMap<>();

	private Timer timer;

	private Map<Long, SearchBucket> searches = new ConcurrentHashMap<>();
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
			logger.error("Could not parse JSON from {}", value.toString());
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

	/**
	 * Commits any pending documents to their respective index.
	 */
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
			logger.trace("create {} {}", entityName, document.toString());
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
					String datasetId = documentObject.getString("dataset.id", null);
					String investigationId = documentObject.getString("investigation.id", null);
					logger.trace("Aggregating {} to {}, {}", jsonFileSize.longValue(), datasetId, investigationId);
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
	 * @param entityId       Icat id of entity to update.
	 * @param index          Index (entity) to update.
	 * @throws IOException
	 */
	private void aggregateFileSize(long sizeToAdd, long sizeToSubtract, long deltaFileCount, String entityId,
			String index)
			throws IOException {
		long deltaFileSize = sizeToAdd - sizeToSubtract;
		if (entityId != null && (deltaFileSize != 0 || deltaFileCount != 0)) {
			IndexBucket indexBucket = indexBuckets.computeIfAbsent(index, k -> new IndexBucket(k));
			for (ShardBucket shardBucket : indexBucket.shardList) {
				shardBucket.commit();
				IndexSearcher searcher = shardBucket.searcherManager.acquire();
				Term idTerm = new Term("id", entityId);
				TopDocs topDocs = searcher.search(new TermQuery(idTerm), 1);
				if (topDocs.totalHits.value == 1) {
					int docId = topDocs.scoreDocs[0].doc;
					Document document = searcher.doc(docId);
					shardBucket.searcherManager.release(searcher);
					Set<String> prunedFields = new HashSet<>();
					List<IndexableField> fieldsToAdd = new ArrayList<>();

					if (deltaFileSize != 0) {
						prunedFields.add("fileSize");
						long oldSize = document.getField("fileSize").numericValue().longValue();
						long newSize = oldSize + deltaFileSize;
						fieldsToAdd.add(new LongPoint("fileSize", newSize));
						fieldsToAdd.add(new StoredField("fileSize", newSize));
						fieldsToAdd.add(new NumericDocValuesField("fileSize", newSize));
					}

					if (deltaFileCount != 0) {
						prunedFields.add("fileCount");
						long oldCount = document.getField("fileCount").numericValue().longValue();
						long newCount = oldCount + deltaFileCount;
						fieldsToAdd.add(new LongPoint("fileCount", newCount));
						fieldsToAdd.add(new StoredField("fileCount", newCount));
						fieldsToAdd.add(new NumericDocValuesField("fileCount", newCount));
					}

					Document newDocument = pruneDocument(prunedFields, document);
					fieldsToAdd.forEach(field -> newDocument.add(field));
					shardBucket.indexWriter.updateDocument(idTerm, facetsConfig.build(newDocument));
					shardBucket.commit();
					break;
				}
				shardBucket.searcherManager.release(searcher);
			}
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
		logger.trace("create {} {}", entityName, document.toString());
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
		Long uid = null;
		try {
			uid = bucketNum.getAndIncrement();
			SearchBucket search = new SearchBucket(this, SearchType.DATAFILE, request, sort, searchAfter);
			searches.put(uid, search);
			return luceneSearchResult("Datafile", search, searchAfter, maxResults);
		} catch (IOException | QueryNodeException e) {
			logger.error("Error", e);
			freeSearcher(uid);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
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

		Long uid = null;
		try {
			uid = bucketNum.getAndIncrement();
			SearchBucket search = new SearchBucket(this, SearchType.DATASET, request, sort, searchAfter);
			searches.put(uid, search);
			return luceneSearchResult("Dataset", search, searchAfter, maxResults);
		} catch (IOException | QueryNodeException e) {
			logger.error("Error", e);
			freeSearcher(uid);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}

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
			String icatId = operationBody.getString("_id");
			try {
				IndexBucket bucket = indexBuckets.computeIfAbsent(entityName.toLowerCase(), k -> new IndexBucket(k));
				if (bucket.locked.get()) {
					throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
							"Lucene locked for " + entityName);
				}
				logger.trace("delete {} {}", entityName, icatId);
				// Special case for filesizes
				Term term = new Term("id", icatId);
				if (aggregateFiles && entityName.equals("Datafile")) {
					long sizeToSubtract = 0;
					for (ShardBucket shardBucket : bucket.shardList) {
						IndexSearcher datafileSearcher = shardBucket.searcherManager.acquire();
						TopDocs topDocs = datafileSearcher.search(new TermQuery(term), 1);
						if (topDocs.totalHits.value == 1) {
							int docId = topDocs.scoreDocs[0].doc;
							Document datasetDocument = datafileSearcher.doc(docId);
							sizeToSubtract = datasetDocument.getField("fileSize").numericValue().longValue();
							if (sizeToSubtract > 0) {
								String datasetId = datasetDocument.getField("dataset.id").stringValue();
								String investigationId = datasetDocument.getField("investigation.id").stringValue();
								aggregateFileSize(0, sizeToSubtract, -1, datasetId, "dataset");
								aggregateFileSize(0, sizeToSubtract, -1, investigationId, "investigation");
							}
							shardBucket.searcherManager.release(datafileSearcher);
							break;
						}
						shardBucket.searcherManager.release(datafileSearcher);
					}
				}
				for (ShardBucket shardBucket : bucket.shardList) {
					shardBucket.indexWriter.deleteDocuments(term);
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
			String parentId;
			if (joinedEntityName.toLowerCase().contains("investigation")) {
				fld = "investigation.id";
				if (entityName.toLowerCase().equals("investigation")) {
					parentId = document.get("id");
				} else {
					parentId = document.get("investigation.id");
				}
			} else {
				fld = entityName.toLowerCase() + ".id";
				parentId = document.get("id");
			}
			joinedSearch.query = new TermQuery(new Term(fld, parentId));
			joinedSearch.sort = new Sort(new SortedNumericSortField("id.long", Type.LONG));
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
		Long uid = null;
		try {
			uid = bucketNum.getAndIncrement();
			SearchBucket search = new SearchBucket(this, SearchType.GENERIC, request, sort, null);
			searches.put(uid, search);
			return luceneFacetResult(entityName, search, searchAfter, maxResults, maxLabels);
		} catch (IOException | QueryNodeException e) {
			logger.error("Error", e);
			freeSearcher(uid);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	/**
	 * Releases all IndexSearchers associated with uid.
	 * 
	 * @param uid Unique Identifier for a set of IndexSearcher to be released.
	 * @throws LuceneException
	 */
	public void freeSearcher(Long uid) throws LuceneException {
		if (uid != null && searches.containsKey(uid)) { // May not be set for internal calls
			Map<String, List<IndexSearcher>> search = searches.get(uid).searcherMap;
			for (Entry<String, List<IndexSearcher>> entry : search.entrySet()) {
				String name = entry.getKey();
				List<IndexSearcher> subReaders = entry.getValue();
				try {
					indexBuckets.computeIfAbsent(name.toLowerCase(), k -> new IndexBucket(k))
							.releaseSearchers(subReaders);
				} catch (IOException e) {
					throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
				}
			}
			searches.remove(uid);
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
		List<IndexSearcher> subSearchers = searcherMap.get(nameLowercase);
		if (subSearchers == null) {
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
		if (subSearchers.size() > 1) {
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
			luceneMaxShardSize = Math.max(props.getPositiveLong("maxShardSize"), new Long(Integer.MAX_VALUE + 1));
			maxSearchTimeSeconds = props.has("maxSearchTimeSeconds") ? props.getPositiveLong("maxSearchTimeSeconds")
					: 5;
			aggregateFiles = props.getBoolean("aggregateFiles", false);

			timer = new Timer("LuceneCommitTimer");
			timer.schedule(new CommitTimerTask(), luceneCommitMillis, luceneCommitMillis);

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
		Long uid = null;
		try {
			uid = bucketNum.getAndIncrement();
			SearchBucket search = new SearchBucket(this, SearchType.INVESTIGATION, request, sort, searchAfter);
			searches.put(uid, search);
			return luceneSearchResult("Investigation", search, searchAfter, maxResults);
		} catch (IOException | QueryNodeException e) {
			logger.error("Error", e);
			freeSearcher(uid);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
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
			@QueryParam("maxId") Long maxId, @QueryParam("delete") Boolean delete) throws LuceneException {
		try {
			entityName = entityName.toLowerCase();
			logger.info("Requesting lock of {} index, minId={}, maxId={}, delete={}", entityName, minId, maxId, delete);
			IndexBucket bucket = indexBuckets.computeIfAbsent(entityName, k -> new IndexBucket(k));

			if (!bucket.locked.compareAndSet(false, true)) {
				String message = "Lucene already locked for " + entityName;
				throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE, message);
			}
			if (delete) {
				for (ShardBucket shardBucket : bucket.shardList) {
					shardBucket.indexWriter.deleteAll();
				}
				// Reset the shardList so we reset the routing
				bucket.shardList = new ArrayList<>();
				ShardBucket shardBucket = bucket.shardList.get(0);
				bucket.shardList.add(shardBucket);
				return;
			}

			for (ShardBucket shardBucket : bucket.shardList) {
				IndexSearcher searcher = shardBucket.searcherManager.acquire();
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
					query = LongPoint.newRangeQuery("id.long", minId + 1, maxId);
				}
				TopDocs topDoc = searcher.search(query, 1);
				if (topDoc.scoreDocs.length != 0) {
					// If we have any results in the populating range, unlock and throw
					bucket.locked.compareAndSet(true, false);
					Document doc = searcher.doc(topDoc.scoreDocs[0].doc);
					String id = doc.get("id");
					String message = "While locking index, found id " + id + " in specified range";
					logger.error(message);
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST, message);
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
			List<IndexSearcher> searchers = getSearchers(search.searcherMap, name);
			logger.debug("Faceting {} with {} after {} ", name, search.query, searchAfter);
			for (IndexSearcher indexSearcher : searchers) {
				FacetsCollector facetsCollector = new FacetsCollector();
				TopDocs results = FacetsCollector.search(indexSearcher, search.query, maxResults, facetsCollector);
				logger.debug("{}", results.totalHits);
				for (FacetedDimension facetedDimension : search.dimensions.values()) {
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
				}
				try {
					if (sparse) {
						// Facet all applicable string fields
						DefaultSortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(
								indexSearcher.getIndexReader());
						Facets facets = new SortedSetDocValuesFacetCounts(state, facetsCollector);
						addFacetResults(maxLabels, search.dimensions, facets);
						logger.trace("Sparse faceting found results for {} dimensions", search.dimensions.size());
					} else if (facetStrings) {
						// Only add facets to the results if they match one of the requested dimensions
						DefaultSortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(
								indexSearcher.getIndexReader());
						Facets facets = new SortedSetDocValuesFacetCounts(state, facetsCollector);
						List<FacetResult> facetResults = facets.getAllDims(maxLabels);
						for (FacetResult facetResult : facetResults) {
							String dimension = facetResult.dim.replace(".keyword", "");
							FacetedDimension facetedDimension = search.dimensions.get(dimension);
							logger.debug("String facets found for {}, requested dimensions were {}", dimension,
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
		}
		// Build results
		JsonObjectBuilder aggregationsBuilder = Json.createObjectBuilder();
		search.dimensions.values().forEach(facetedDimension -> facetedDimension.buildResponse(aggregationsBuilder));
		String aggregations = Json.createObjectBuilder().add("aggregations", aggregationsBuilder).build().toString();
		logger.debug("aggregations: {}", aggregations);
		return aggregations;
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
						String fieldName = sortField.getField();
						if (fieldName == null) {
							// SCORE sorting will have a null fieldName
							if (Float.isFinite(lastDoc.score)) {
								gen.write(lastDoc.score);
							}
							continue;
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
									gen.write(new Long(indexableField.stringValue()));
								}
								break;
							case DOUBLE:
								if (indexableField.numericValue() != null) {
									gen.write(indexableField.numericValue().doubleValue());
								} else if (indexableField.stringValue() != null) {
									gen.write(new Double(indexableField.stringValue()));
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
		logger.trace("Json returned {}", baos.toString());
		return baos.toString();
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
				indexSearcher.search(search.query, collector);
				TopFieldDocs topDocs = topFieldCollector.topDocs();
				if (search.scored) {
					TopFieldCollector.populateScores(topDocs.scoreDocs, indexSearcher, search.query);
				}
				shardHits.add(topDocs);
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

	/**
	 * Extracts key/value pairs from json to add to the Lucene document.
	 * 
	 * @param json     JsonObject containing the field/value pairs to be added.
	 * @param document Lucene Document being built.
	 * @param key      Name of the field to be added.
	 */
	private void addField(JsonObject json, Document document, String key) {
		// SortedDocValuesField need to be indexed in addition to indexing a Field for
		// searching/storing, so deal with that first
		addSortField(json, document, key);

		// Likewise, faceted fields should be considered separately
		if (DocumentMapping.facetFields.contains(key)) {
			document.add(new SortedSetDocValuesFacetField(key + ".keyword", json.getString(key)));
			document.add(new StringField(key + ".keyword", json.getString(key), Store.NO));
		}

		if (DocumentMapping.doubleFields.contains(key)) {
			Double value = json.getJsonNumber(key).doubleValue();
			document.add(new DoublePoint(key, value));
			document.add(new StoredField(key, value));
		} else if (DocumentMapping.longFields.contains(key)) {
			Long value = json.getJsonNumber(key).longValueExact();
			document.add(new LongPoint(key, value));
			document.add(new StoredField(key, value));
		} else if (DocumentMapping.textFields.contains(key)) {
			document.add(new TextField(key, json.getString(key), Store.YES));
		} else {
			document.add(new StringField(key, json.getString(key), Store.YES));
		}

		// Whenever the units are set or changed, convert to SI
		if (key.equals("type.units")) {
			String unitString = json.getString("type.units");
			convertValue(document, json, unitString, "numericValue");
			convertValue(document, json, unitString, "rangeTop");
			convertValue(document, json, unitString, "rangeBottom");
		}
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
	 * Adds field to document taking its typing, sorting and faceting into account.
	 * 
	 * @param field    Lucene IndexableField to add to the document.
	 * @param document Lucene Document to add the field to.
	 */
	private void addField(IndexableField field, Document document) {
		// SortedDocValuesField need to be indexed in addition to indexing a Field for
		// searching/storing, so deal with that first
		addSortField(field, document);
		String key = field.name();

		// Likewise, faceted fields should be considered separately
		if (DocumentMapping.facetFields.contains(key)) {
			String value = field.stringValue();
			document.add(new SortedSetDocValuesFacetField(key + ".keyword", value));
			document.add(new StringField(key + ".keyword", value, Store.NO));
		}

		if (DocumentMapping.doubleFields.contains(key)) {
			Double value = field.numericValue().doubleValue();
			document.add(new DoublePoint(key, value));
			document.add(new StoredField(key, value));
		} else if (DocumentMapping.longFields.contains(key)) {
			Long value = field.numericValue().longValue();
			document.add(new LongPoint(key, value));
			document.add(new StoredField(key, value));
		} else if (DocumentMapping.textFields.contains(key)) {
			document.add(new TextField(key, field.stringValue(), Store.YES));
		} else {
			document.add(new StringField(key, field.stringValue(), Store.YES));
		}
	}

	/**
	 * Extracts key/value pairs from json to add to the Lucene document. Handles
	 * fields which need to be sortable.
	 * 
	 * @param json     JsonObject containing the field/value pairs to be added.
	 * @param document Lucene Document being built.
	 * @param key      Name of the field to be added.
	 */
	private void addSortField(JsonObject json, Document document, String key) {
		if (DocumentMapping.sortFields.contains(key)) {
			if (key.equals("id")) {
				// Id is a special case, as we need to to be SORTED as a byte ref to allow joins
				// but also SORTED_NUMERIC to ensure a deterministic order to results
				Long value = new Long(json.getString(key));
				document.add(new NumericDocValuesField("id.long", value));
				document.add(new StoredField("id.long", value));
				document.add(new LongPoint("id.long", value));
			}
			if (DocumentMapping.longFields.contains(key)) {
				document.add(new NumericDocValuesField(key, json.getJsonNumber(key).longValueExact()));
			} else if (DocumentMapping.doubleFields.contains(key)) {
				long sortableLong = NumericUtils.doubleToSortableLong(json.getJsonNumber(key).doubleValue());
				document.add(new NumericDocValuesField(key, sortableLong));
			} else {
				document.add(new SortedDocValuesField(key, new BytesRef(json.getString(key))));
			}
		}
	}

	/**
	 * Re-adds the content of a Lucene IndexableField to a Lucene Document. This is
	 * needed when updating Documents to ensure sorting is not lost.
	 * 
	 * @param field    Lucene IndexableField to be re-added to the document.
	 * @param document Lucene Document being built.
	 */
	private void addSortField(IndexableField field, Document document) {
		String key = field.name();
		if (DocumentMapping.sortFields.contains(key)) {
			if (key.equals("id")) {
				// Id is a special case, as we need to to be SORTED as a byte ref to allow joins
				// but also SORTED_NUMERIC to ensure a deterministic order to results
				Long value = new Long(field.stringValue());
				document.add(new NumericDocValuesField("id.long", value));
				document.add(new StoredField("id.long", value));
				document.add(new LongPoint("id.long", value));
			}
			if (DocumentMapping.longFields.contains(key)) {
				document.add(new NumericDocValuesField(key, field.numericValue().longValue()));
			} else if (DocumentMapping.doubleFields.contains(key)) {
				long sortableLong = NumericUtils.doubleToSortableLong(field.numericValue().doubleValue());
				document.add(new NumericDocValuesField(key, sortableLong));
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
				addField(field, newDocument);
			}
		}
		return newDocument;
	}

	/**
	 * Unlocks the specified index after population, commiting all pending documents
	 * and
	 * allowing normal modify operations again.
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
	 * locked
	 * for another operation.
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
			String icatId = operationBody.getString("_id");
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
						TopDocs topDocs = datafileSearcher.search(new TermQuery(new Term("id", icatId)), 1);
						if (topDocs.totalHits.value == 1) {
							int docId = topDocs.scoreDocs[0].doc;
							Document datasetDocument = datafileSearcher.doc(docId);
							sizeToSubtract = datasetDocument.getField("fileSize").numericValue().longValue();
							long sizeToAdd = jsonFileSize.longValueExact();
							if (sizeToAdd != sizeToSubtract) {
								String datasetId = documentObject.getString("dataset.id", null);
								String investigationId = documentObject.getString("investigation.id", null);
								aggregateFileSize(sizeToAdd, sizeToSubtract, 0, datasetId, "dataset");
								aggregateFileSize(sizeToAdd, sizeToSubtract, 0, investigationId, "investigation");
							}
							break;
						}
					}
				}
			}
			logger.trace("update: {}", document);
			bucket.updateDocument(new Term("id", icatId), facetsConfig.build(document));
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
	private void updateByRelation(JsonObject operationBody, Boolean delete)
			throws LuceneException, NumberFormatException, IOException {
		for (DocumentMapping.ParentRelationship parentRelationship : DocumentMapping.relationships
				.get(operationBody.getString("_index"))) {
			String childId = operationBody.getString("_id");
			IndexBucket bucket = indexBuckets.computeIfAbsent(parentRelationship.parentName.toLowerCase(),
					k -> new IndexBucket(k));
			if (bucket.locked.get()) {
				throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
						"Lucene locked for " + parentRelationship.parentName);
			}
			IndexSearcher searcher = getSearcher(new HashMap<>(), parentRelationship.parentName);

			int blockSize = 10000;
			TermQuery query = new TermQuery(new Term(parentRelationship.joiningField, childId));
			Sort sort = new Sort(new SortField("id", Type.STRING));
			ScoreDoc[] scoreDocs = searcher.search(query, blockSize, sort).scoreDocs;
			while (scoreDocs.length != 0) {
				TopDocs topDocs = searcher.search(query, blockSize);
				for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
					Document oldDocument = searcher.doc(scoreDoc.doc);
					String parentId = oldDocument.get("id");
					Document newDocument = delete ? pruneDocument(parentRelationship.fields, oldDocument)
							: updateDocument(operationBody.getJsonObject("doc"), oldDocument);
					logger.trace("updateByRelation: {}", newDocument);
					bucket.updateDocument(new Term("id", parentId), facetsConfig.build(newDocument));
				}
				scoreDocs = searcher.searchAfter(scoreDocs[scoreDocs.length - 1], query, blockSize, sort).scoreDocs;
			}
		}
	}

}
