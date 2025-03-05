package org.icatproject.lucene;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Map.Entry;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import jakarta.json.JsonValue.ValueType;
import jakarta.servlet.http.HttpServletRequest;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.Range;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.icatproject.lucene.exceptions.LuceneException;
import org.icatproject.utils.IcatUnits.Value;

/**
 * Bucket for information relating to a single search.
 */
public class SearchBucket {

    public enum SearchType {
        DATAFILE, DATASET, INVESTIGATION, GENERIC
    }

    private Lucene lucene;
    public Map<String, List<IndexSearcher>> searcherMap;
    public String user;
    public Query query;
    public Sort sort;
    public FieldDoc searchAfter;
    public boolean scored;
    public Set<String> fields = new HashSet<>();
    public Map<String, Set<String>> joinedFields = new HashMap<>();
    public Map<String, FacetedDimension> dimensions = new HashMap<>();
    private static final SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");

    static {
        TimeZone tz = TimeZone.getTimeZone("GMT");
        df.setTimeZone(tz);
    }

    /**
     * Creates an empty search bucket.
     * 
     * @param lucene IcatLucene instance.
     */
    public SearchBucket(Lucene lucene) {
        this.lucene = lucene;
        searcherMap = new HashMap<>();
    }

    /**
     * Creates a new search from the provided request and Url parameters.
     * 
     * @param lucene      IcatLucene instance.
     * @param searchType  The SearchType determines how the query is built for
     *                    specific entities.
     * @param request     Incoming Http request containing the query as Json.
     * @param sort        Sort criteria as a Json encoded string.
     * @param searchAfter The last FieldDoc of a previous search, encoded as Json.
     * @throws LuceneException
     * @throws IOException
     * @throws QueryNodeException
     */
    public SearchBucket(Lucene lucene, SearchType searchType, HttpServletRequest request, String sort,
            String searchAfter) throws LuceneException, IOException, QueryNodeException {
        this(lucene, searchType, Json.createReader(request.getInputStream()).readObject(), sort, searchAfter);
    }

    /**
     * Creates a new search from the provided request and Url parameters.
     * 
     * @param lucene      IcatLucene instance.
     * @param searchType  The SearchType determines how the query is built for
     *                    specific entities.
     * @param object      Incoming query as Json.
     * @param sort        Sort criteria as a Json encoded string.
     * @param searchAfter The last FieldDoc of a previous search, encoded as Json.
     * @throws LuceneException
     * @throws IOException
     * @throws QueryNodeException
     */
    public SearchBucket(Lucene lucene, SearchType searchType, JsonObject object, String sort,
            String searchAfter) throws LuceneException, IOException, QueryNodeException {
        this.lucene = lucene;
        searcherMap = new HashMap<>();
        parseSort(sort);
        try {
            parseFields(object);
            parseDimensions(object);
            JsonObject jsonQuery = object.getJsonObject("query");
            switch (searchType) {
                case GENERIC:
                    parseGenericQuery(jsonQuery);
                    return;
                case DATAFILE:
                    parseDatafileQuery(searchAfter, jsonQuery);
                    return;
                case DATASET:
                    parseDatasetQuery(searchAfter, jsonQuery);
                    return;
                case INVESTIGATION:
                    parseInvestigationQuery(searchAfter, jsonQuery);
                    return;
            }
        } catch (QueryNodeParseException e) {
            String message = "Search term could not be parsed due to syntax errors";
            throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST, message);
        }
    }

    /**
     * By design, Lucene does not apply Analyzers to WildcardQueries in order to avoid
     * stemming and tokenising interfering with the user's intended query. A consequence
     * of this is that the filter which lowercases the search text is not applied. This
     * means a search of ABC* would not match a Document with source ABCD as the latter
     * is lowercased when indexed, and the match between ABC* and abcd is not made.
     * 
     * To give a more intuitive search for users, iterate over all the nested levels of
     * the query, extract any instanceof WildcardQuery, and case the text to lowercase.
     * Not that we cannot naively lowercase all the text given by the user, as in
     * principle it may contain field names which are case sensitive (e.g. visitId and
     * not visitid).
     * 
     * @param query Any Lucene Query
     * @return The same query but with the text of any WildcardQueries lowercased.
     */
    private Query lowercaseWildcardQueries(Query query) {
        if (query instanceof WildcardQuery) {
            Term term = ((WildcardQuery) query).getTerm();
            String field = term.field();
            String text = term.text();
            return new WildcardQuery(new Term(field, text.toLowerCase()));
        } else if (query instanceof PrefixQuery) {
            Term term = ((PrefixQuery) query).getPrefix();
            String field = term.field();
            String text = term.text();
            return new PrefixQuery(new Term(field, text.toLowerCase()));
        } else if (query instanceof BooleanQuery) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (BooleanClause clause : (BooleanQuery) query) {
                Query nestedQuery = clause.getQuery();
                Query processedNestedQuery = lowercaseWildcardQueries(nestedQuery);
                builder.add(processedNestedQuery, clause.getOccur());
            }
            return builder.build();
        } else {
            return query;
        }
    }

    private void parseDatafileQuery(String searchAfter, JsonObject jsonQuery)
            throws LuceneException, IOException, QueryNodeException {
        BooleanQuery.Builder luceneQuery = new BooleanQuery.Builder();
        parseSearchAfter(searchAfter);
        buildFilterQueries("datafile", jsonQuery, luceneQuery);

        user = jsonQuery.getString("user", null);
        if (user != null) {
            buildUserNameQuery(luceneQuery, "investigation.id");
        }

        String text = jsonQuery.getString("text", null);
        if (text != null) {
            Query parsedQuery = DocumentMapping.datafileParser.parse(text, null);
            Query lowercasedQuery = lowercaseWildcardQueries(parsedQuery);
            luceneQuery.add(lowercasedQuery, Occur.MUST);
        }

        buildDateRanges(luceneQuery, jsonQuery, "lower", "upper", "date");

        if (jsonQuery.containsKey("parameters")) {
            JsonArray parameters = jsonQuery.getJsonArray("parameters");
            IndexSearcher datafileParameterSearcher = lucene.getSearcher(searcherMap, "DatafileParameter");
            for (JsonValue p : parameters) {
                BooleanQuery.Builder paramQuery = parseParameter(p);
                Query toQuery = JoinUtil.createJoinQuery("datafile.id", false, "id", Long.class, paramQuery.build(),
                        datafileParameterSearcher, ScoreMode.None);
                luceneQuery.add(toQuery, Occur.MUST);
            }
        }
        query = maybeEmptyQuery(luceneQuery);
    }

    private void parseDatasetQuery(String searchAfter, JsonObject jsonQuery)
            throws LuceneException, IOException, QueryNodeException {
        BooleanQuery.Builder luceneQuery = new BooleanQuery.Builder();
        parseSearchAfter(searchAfter);
        buildFilterQueries("dataset", jsonQuery, luceneQuery);

        user = jsonQuery.getString("user", null);
        if (user != null) {
            buildUserNameQuery(luceneQuery, "investigation.id");
        }

        String text = jsonQuery.getString("text", null);
        if (text != null) {
            Query parsedQuery = DocumentMapping.datasetParser.parse(text, null);
            Query lowercasedQuery = lowercaseWildcardQueries(parsedQuery);
            luceneQuery.add(lowercasedQuery, Occur.MUST);
        }

        buildDateRanges(luceneQuery, jsonQuery, "lower", "upper", "startDate", "endDate");

        if (jsonQuery.containsKey("parameters")) {
            JsonArray parameters = jsonQuery.getJsonArray("parameters");
            IndexSearcher parameterSearcher = lucene.getSearcher(searcherMap, "DatasetParameter");
            for (JsonValue p : parameters) {
                BooleanQuery.Builder paramQuery = parseParameter(p);
                Query toQuery = JoinUtil.createJoinQuery("dataset.id", false, "id", Long.class, paramQuery.build(),
                        parameterSearcher, ScoreMode.None);
                luceneQuery.add(toQuery, Occur.MUST);
            }
        }
        query = maybeEmptyQuery(luceneQuery);
    }

    private void parseInvestigationQuery(String searchAfter, JsonObject jsonQuery)
            throws LuceneException, IOException, QueryNodeException {
        BooleanQuery.Builder luceneQuery = new BooleanQuery.Builder();
        parseSearchAfter(searchAfter);
        buildFilterQueries("investigation", jsonQuery, luceneQuery);

        user = jsonQuery.getString("user", null);
        if (user != null) {
            buildUserNameQuery(luceneQuery, "id");
        }

        String text = jsonQuery.getString("text", null);
        if (text != null) {
            Builder textBuilder = new BooleanQuery.Builder();
            Query parsedQuery = DocumentMapping.investigationParser.parse(text, null);
            Query lowercasedQuery = lowercaseWildcardQueries(parsedQuery);
            textBuilder.add(lowercasedQuery, Occur.SHOULD);

            IndexSearcher sampleSearcher = lucene.getSearcher(searcherMap, "Sample");
            parsedQuery = DocumentMapping.sampleParser.parse(text, null);
            lowercasedQuery = lowercaseWildcardQueries(parsedQuery);
            Query joinedSampleQuery = JoinUtil.createJoinQuery("sample.investigation.id", false, "id", Long.class,
                    lowercasedQuery, sampleSearcher, ScoreMode.Avg);
            textBuilder.add(joinedSampleQuery, Occur.SHOULD);
            luceneQuery.add(textBuilder.build(), Occur.MUST);
        }

        buildDateRanges(luceneQuery, jsonQuery, "lower", "upper", "startDate", "endDate");

        if (jsonQuery.containsKey("parameters")) {
            JsonArray parameters = jsonQuery.getJsonArray("parameters");
            IndexSearcher parameterSearcher = lucene.getSearcher(searcherMap, "InvestigationParameter");
            for (JsonValue p : parameters) {
                BooleanQuery.Builder paramQuery = parseParameter(p);
                Query toQuery = JoinUtil.createJoinQuery("investigation.id", false, "id", Long.class,
                        paramQuery.build(),
                        parameterSearcher, ScoreMode.None);
                luceneQuery.add(toQuery, Occur.MUST);
            }
        }

        String userFullName = jsonQuery.getString("userFullName", null);
        if (userFullName != null) {
            BooleanQuery.Builder userFullNameQuery = new BooleanQuery.Builder();
            Query parsedQuery = DocumentMapping.genericParser.parse(userFullName, "user.fullName");
            Query lowercasedQuery = lowercaseWildcardQueries(parsedQuery);
            userFullNameQuery.add(lowercasedQuery, Occur.MUST);
            IndexSearcher investigationUserSearcher = lucene.getSearcher(searcherMap, "InvestigationUser");
            Query toQuery = JoinUtil.createJoinQuery("investigation.id", false, "id", Long.class,
                    userFullNameQuery.build(),
                    investigationUserSearcher, ScoreMode.None);
            luceneQuery.add(toQuery, Occur.MUST);
        }
        query = maybeEmptyQuery(luceneQuery);
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
    private void buildDateRanges(Builder queryBuilder, JsonObject queryJson, String lowerKey, String upperKey,
            String... fields) throws LuceneException {
        long lower = parseDate(queryJson, lowerKey, 0);
        long upper = parseDate(queryJson, upperKey, 59999);
        // Only build the query if at least one of the dates is defined
        if (lower != Long.MIN_VALUE || upper != Long.MAX_VALUE) {
            for (String field : fields) {
                queryBuilder.add(LongPoint.newRangeQuery(field, lower, upper), Occur.MUST);
            }
        }
    }

    /**
     * Builds Term queries (exact string matches without tokenizing) Range queries
     * or Nested/Joined queries from the filter
     * object in the query request.
     * 
     * @param requestedQuery Json object containing details of the query.
     * @param queryBuilder   Builder for the overall boolean query to be build.
     * @throws LuceneException If the values in the filter object are neither STRING
     *                         nor ARRAY of STRING.
     * @throws IOException
     */
    private void buildFilterQueries(String target, JsonObject requestedQuery, Builder queryBuilder)
            throws LuceneException, IOException {
        if (requestedQuery.containsKey("filter")) {
            JsonObject filterObject = requestedQuery.getJsonObject("filter");
            for (String key : filterObject.keySet()) {
                JsonValue value = filterObject.get(key);
                ValueType valueType = value.getValueType();
                int i = key.indexOf(".");
                String filterTarget = i == -1 ? key : key.substring(0, i);
                String fld = key.substring(i + 1);
                Query dimensionQuery;
                if (valueType.equals(ValueType.ARRAY)) {
                    Builder builder = new BooleanQuery.Builder();
                    // If the key was just a nested entity (no ".") then we should FILTER all of our
                    // queries on that entity.
                    Occur occur = i == -1 ? Occur.FILTER : Occur.SHOULD;
                    for (JsonValue arrayValue : filterObject.getJsonArray(key)) {
                        Query arrayQuery = parseFilter(target, fld, arrayValue);
                        builder.add(arrayQuery, occur);
                    }
                    dimensionQuery = builder.build();
                } else {
                    dimensionQuery = parseFilter(target, fld, value);
                }
                // Nest the dimension query if needed
                if (i != -1 && !target.equals(filterTarget)) {
                    // If we are targeting a different entity, nest the entire array as SHOULD
                    // BUT only if we haven't already nested the queries (as we do when the key was
                    // just a nested entity)
                    IndexSearcher nestedSearcher = lucene.getSearcher(searcherMap, filterTarget);
                    Query nestedQuery;
                    if (filterTarget.equals("sample") && target.equals("investigation")) {
                        nestedQuery = JoinUtil.createJoinQuery("sample.investigation.id", false, "id", Long.class,
                                dimensionQuery, nestedSearcher, ScoreMode.None);
                    } else if (filterTarget.toLowerCase().equals("investigationinstrument") && !target.equals("investigation")) {
                        nestedQuery = JoinUtil.createJoinQuery("investigation.id", false, "investigation.id", Long.class, dimensionQuery,
                                nestedSearcher, ScoreMode.None);
                    } else {
                        nestedQuery = JoinUtil.createJoinQuery(target + ".id", false, "id", Long.class, dimensionQuery,
                                nestedSearcher, ScoreMode.None);
                    }
                    queryBuilder.add(nestedQuery, Occur.FILTER);
                } else {
                    // Otherwise, just add as SHOULD to the main query directly
                    queryBuilder.add(dimensionQuery, Occur.FILTER);
                }
            }
        }
    }

    /**
     * Parses a single filter field value pair into Lucene objects. Can handle
     * simple strings, range objects or nested filters.
     * 
     * @param target The target entity of the search, but not necessarily this
     *               filter
     * @param fld    The field to apply the query to
     * @param value  JsonValue (JsonString or JsonObject) to parse a Lucene Query
     *               from
     * @return A Lucene Query object parsed from the provided value
     * @throws IOException
     * @throws LuceneException
     */
    private Query parseFilter(String target, String fld, JsonValue value) throws IOException, LuceneException {
        ValueType valueType = value.getValueType();
        switch (valueType) {
            case STRING:
                // Simplest case involving a single field/value pair
                return new TermQuery(new Term(fld + ".keyword", ((JsonString) value).getString()));

            case OBJECT:
                JsonObject valueObject = (JsonObject) value;
                if (valueObject.containsKey("filter")) {
                    // Parse a nested query
                    IndexSearcher nestedSearcher = lucene.getSearcher(searcherMap, fld);
                    List<JsonObject> nestedFilters = valueObject.getJsonArray("filter").getValuesAs(JsonObject.class);
                    Builder nestedBoolBuilder = new BooleanQuery.Builder();
                    nestedFilters.forEach(nestedFilter -> {
                        String nestedField = nestedFilter.getString("field");
                        if (nestedFilter.containsKey("value")) {
                            Term term = new Term(nestedField + ".keyword", nestedFilter.getString("value"));
                            TermQuery query = new TermQuery(term);
                            nestedBoolBuilder.add(query, Occur.FILTER);
                        } else if (nestedFilter.containsKey("exact")) {
                            buildNestedExactQuery(nestedField, nestedFilter, nestedBoolBuilder);
                        } else {
                            buildNestedRangeQuery(nestedField, nestedFilter, nestedBoolBuilder);
                        }
                    });
                    if (fld.contains("sample") && !target.equals("investigation")) {
                        // Datasets and Datafiles join by sample.id on both fields
                        return JoinUtil.createJoinQuery("sample.id", false, "sample.id", Long.class,
                                nestedBoolBuilder.build(), nestedSearcher, ScoreMode.None);
                    } else if (fld.equals("sampleparameter") && target.equals("investigation")) {
                        Query sampleQuery = JoinUtil.createJoinQuery("sample.id", false, "sample.id", Long.class,
                                nestedBoolBuilder.build(), nestedSearcher, ScoreMode.None);
                        return JoinUtil.createJoinQuery("sample.investigation.id", false, "id", Long.class, sampleQuery,
                                lucene.getSearcher(searcherMap, "sample"), ScoreMode.None);
                    } else {
                        return JoinUtil.createJoinQuery(target + ".id", false, "id", Long.class,
                                nestedBoolBuilder.build(), nestedSearcher, ScoreMode.None);
                    }
                } else {
                    // Single range of values for a field
                    JsonNumber from = valueObject.getJsonNumber("from");
                    JsonNumber to = valueObject.getJsonNumber("to");
                    if (DocumentMapping.longFields.contains(fld)) {
                        return LongPoint.newRangeQuery(fld, from.longValueExact(), to.longValueExact());
                    } else {
                        return DoublePoint.newRangeQuery(fld, from.doubleValue(), to.doubleValue());
                    }
                }

            default:
                throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
                        "filter object values should be STRING or OBJECT, but were " + valueType);
        }
    }

    /**
     * Builds an exact numeric query, intended for use with numeric or date/time
     * parameters.
     * 
     * @param fld         Name of the field to apply the range to.
     * @param valueObject JsonObject containing "exact", and optionally "units"
     *                    as keys for an exact value.
     * @param builder     BooleanQuery.Builder for the nested query
     */
    private void buildNestedExactQuery(String fld, JsonObject valueObject, BooleanQuery.Builder builder) {
        if (DocumentMapping.longFields.contains(fld)) {
            long exact = valueObject.getJsonNumber("exact").longValueExact();
            builder.add(LongPoint.newExactQuery(fld, exact), Occur.FILTER);
        } else {
            Builder rangeBuilder = new BooleanQuery.Builder();
            Builder exactOrRangeBuilder = new BooleanQuery.Builder();
            double exact = valueObject.getJsonNumber("exact").doubleValue();
            String units = valueObject.getString("units", null);
            if (units != null) {
                Value exactValue = lucene.icatUnits.convertValueToSiUnits(exact, units);
                if (exactValue != null) {
                    // If we were able to parse the units, apply query to the SI value
                    Query topQuery = DoublePoint.newRangeQuery("rangeTopSI", exactValue.numericalValue,
                            Double.POSITIVE_INFINITY);
                    Query bottomQuery = DoublePoint.newRangeQuery("rangeBottomSI", Double.NEGATIVE_INFINITY,
                            exactValue.numericalValue);
                    Query exactQuery = DoublePoint.newExactQuery(fld + "SI", exactValue.numericalValue);
                    rangeBuilder.add(topQuery, Occur.FILTER);
                    rangeBuilder.add(bottomQuery, Occur.FILTER);
                    exactOrRangeBuilder.add(rangeBuilder.build(), Occur.SHOULD);
                    exactOrRangeBuilder.add(exactQuery, Occur.SHOULD);
                    builder.add(exactOrRangeBuilder.build(), Occur.FILTER);
                } else {
                    // If units could not be parsed, make them part of the query on the raw data
                    rangeBuilder.add(DoublePoint.newRangeQuery("rangeTop", exact, Double.POSITIVE_INFINITY),
                            Occur.FILTER);
                    rangeBuilder.add(DoublePoint.newRangeQuery("rangeBottom", Double.NEGATIVE_INFINITY, exact),
                            Occur.FILTER);
                    exactOrRangeBuilder.add(rangeBuilder.build(), Occur.SHOULD);
                    exactOrRangeBuilder.add(DoublePoint.newExactQuery(fld, exact), Occur.SHOULD);
                    builder.add(exactOrRangeBuilder.build(), Occur.FILTER);
                    builder.add(new TermQuery(new Term("type.units", units)), Occur.FILTER);
                }
            } else {
                // If units were not provided, just apply to the raw data
                rangeBuilder.add(DoublePoint.newRangeQuery("rangeTop", exact, Double.POSITIVE_INFINITY), Occur.FILTER);
                rangeBuilder.add(DoublePoint.newRangeQuery("rangeBottom", Double.NEGATIVE_INFINITY, exact),
                        Occur.FILTER);
                exactOrRangeBuilder.add(rangeBuilder.build(), Occur.SHOULD);
                exactOrRangeBuilder.add(DoublePoint.newExactQuery(fld, exact), Occur.SHOULD);
                builder.add(exactOrRangeBuilder.build(), Occur.FILTER);
            }
        }
    }

    /**
     * Builds a range query, intended for use with numeric or date/time parameters.
     * 
     * @param fld         Name of the field to apply the range to.
     * @param valueObject JsonObject containing "from", "to" and optionally "units"
     *                    as keys for a range of values.
     * @param builder     BooleanQuery.Builder for the nested query
     */
    private void buildNestedRangeQuery(String fld, JsonObject valueObject, BooleanQuery.Builder builder) {
        if (DocumentMapping.longFields.contains(fld)) {
            long from = Long.MIN_VALUE;
            long to = Long.MAX_VALUE;
            try {
                from = valueObject.getJsonNumber("from").longValueExact();
            } catch (ArithmeticException e) {
                // pass
            }
            try {
                to = valueObject.getJsonNumber("to").longValueExact();
            } catch (ArithmeticException e) {
                // pass
            }
            builder.add(LongPoint.newRangeQuery(fld, from, to), Occur.FILTER);
        } else {
            double from = valueObject.getJsonNumber("from").doubleValue();
            double to = valueObject.getJsonNumber("to").doubleValue();
            String units = valueObject.getString("units", null);
            if (units != null) {
                Value fromValue = lucene.icatUnits.convertValueToSiUnits(from, units);
                Value toValue = lucene.icatUnits.convertValueToSiUnits(to, units);
                if (fromValue != null && toValue != null) {
                    // If we were able to parse the units, apply query to the SI value
                    Query rangeQuery = DoublePoint.newRangeQuery(fld + "SI", fromValue.numericalValue,
                            toValue.numericalValue);
                    builder.add(rangeQuery, Occur.FILTER);
                } else {
                    // If units could not be parsed, make them part of the query on the raw data
                    builder.add(DoublePoint.newRangeQuery(fld, from, to), Occur.FILTER);
                    builder.add(new TermQuery(new Term("type.units", units)), Occur.FILTER);
                }
            } else {
                // If units were not provided, just apply to the raw data
                builder.add(DoublePoint.newRangeQuery(fld, from, to), Occur.FILTER);
            }
        }
    }

    /**
     * Builds a query against InvestigationUser and InstrumentScientist entities
     * using the provided userName.
     * 
     * @param userName    The value of the user.name field to query for.
     * @param luceneQuery BooleanQuery.Builder in use for main entity query.
     * @param toField     The field on the main entity to join to, practically
     *                    either "id" or "investigation.id".
     * @throws IOException
     * @throws LuceneException
     */
    private void buildUserNameQuery(BooleanQuery.Builder luceneQuery, String toField)
            throws IOException, LuceneException {
        TermQuery fromQuery = new TermQuery(new Term("user.name", user));
        Query investigationUserQuery = JoinUtil.createJoinQuery("investigation.id", false, toField, Long.class,
                fromQuery, lucene.getSearcher(searcherMap, "InvestigationUser"), ScoreMode.None);
        Query instrumentScientistQuery = JoinUtil.createJoinQuery("instrument.id", false, "instrument.id", Long.class,
                fromQuery, lucene.getSearcher(searcherMap, "InstrumentScientist"), ScoreMode.None);
        Query investigationInstrumentQuery = JoinUtil.createJoinQuery("investigation.id", false, toField, Long.class,
                instrumentScientistQuery, lucene.getSearcher(searcherMap, "InvestigationInstrument"), ScoreMode.None);
        Builder userNameQueryBuilder = new BooleanQuery.Builder();
        userNameQueryBuilder.add(investigationUserQuery, Occur.SHOULD).add(investigationInstrumentQuery, Occur.SHOULD);
        luceneQuery.add(userNameQueryBuilder.build(), Occur.MUST);
    }

    /**
     * Converts String into number of ms since epoch.
     * 
     * @param value String representing a Date in the format "yyyyMMddHHmm".
     * @return Number of ms since epoch.
     * @throws java.text.ParseException
     */
    protected static long decodeTime(String value) throws java.text.ParseException {
        synchronized (df) {
            return df.parse(value).getTime();
        }
    }

    /**
     * Either builds the query from the provided builder, or creates a
     * MatchAllDocsQuery to use if the Builder was empty.
     * 
     * @param luceneQuery BooleanQuery.Builder
     * @return Lucene Query
     */
    private Query maybeEmptyQuery(Builder luceneQuery) {
        Query query = luceneQuery.build();
        if (query.toString().isEmpty()) {
            query = new MatchAllDocsQuery();
        }
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
    private long parseDate(JsonObject jsonObject, String key, int offset) throws LuceneException {
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
        // If the key wasn't present, use eiter MIN_VALUE or MAX_VALUE based on whether
        // we need to offset the date. This is useful for half open ranges.
        if (offset == 0) {
            return Long.MIN_VALUE;
        } else {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Parses dimensions to apply faceting to from the incoming Json. If ranges are
     * specified, these are also parsed.
     * 
     * @param jsonObject Json from incoming search request.
     * @throws LuceneException
     */
    private void parseDimensions(JsonObject jsonObject) throws LuceneException {
        if (jsonObject.containsKey("dimensions")) {
            List<JsonObject> dimensionObjects = jsonObject.getJsonArray("dimensions").getValuesAs(JsonObject.class);
            for (JsonObject dimensionObject : dimensionObjects) {
                if (!dimensionObject.containsKey("dimension")) {
                    throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
                            "'dimension' not specified for facet request " + dimensionObject);
                }
                String dimension = dimensionObject.getString("dimension");
                FacetedDimension facetDimensionRequest = new FacetedDimension(dimension);
                if (dimensionObject.containsKey("ranges")) {
                    List<Range> ranges = facetDimensionRequest.getRanges();
                    List<JsonObject> jsonRanges = dimensionObject.getJsonArray("ranges").getValuesAs(JsonObject.class);
                    if (DocumentMapping.longFields.contains(dimension)) {
                        for (JsonObject range : jsonRanges) {
                            long lower = Long.MIN_VALUE;
                            long upper = Long.MAX_VALUE;
                            if (range.containsKey("from")) {
                                lower = range.getJsonNumber("from").longValueExact();
                            }
                            if (range.containsKey("to")) {
                                upper = range.getJsonNumber("to").longValueExact();
                            }
                            String label = lower + "-" + upper;
                            if (range.containsKey("key")) {
                                label = range.getString("key");
                            }
                            ranges.add(new LongRange(label, lower, true, upper, false));
                        }
                    } else if (DocumentMapping.doubleFields.contains(dimension)) {
                        for (JsonObject range : jsonRanges) {
                            double lower = Double.MIN_VALUE;
                            double upper = Double.MAX_VALUE;
                            if (range.containsKey("from")) {
                                lower = range.getJsonNumber("from").doubleValue();
                            }
                            if (range.containsKey("to")) {
                                upper = range.getJsonNumber("to").doubleValue();
                            }
                            String label = lower + "-" + upper;
                            if (range.containsKey("key")) {
                                label = range.getString("key");
                            }
                            ranges.add(new DoubleRange(label, lower, true, upper, false));
                        }
                    } else {
                        throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
                                "'ranges' specified for dimension " + dimension
                                        + " but this is not a supported numeric field");
                    }
                }
                dimensions.put(dimension, facetDimensionRequest);
            }
        }
    }

    /**
     * Parses the fields to return with the search results from Json.
     * 
     * @param jsonObject The Json from the search request.
     * @throws LuceneException If the parsing fails.
     */
    public void parseFields(JsonObject jsonObject) throws LuceneException {
        if (jsonObject.containsKey("fields")) {
            List<JsonString> fieldStrings = jsonObject.getJsonArray("fields").getValuesAs(JsonString.class);
            for (JsonString jsonString : fieldStrings) {
                String[] splitString = jsonString.getString().split(" ");
                if (splitString.length == 1) {
                    // Fields without a space apply directly to the target entity
                    fields.add(splitString[0]);
                } else if (splitString.length == 2) {
                    // Otherwise, the first element is the target of a join, with the second being a
                    // field on that joined entity.
                    if (joinedFields.containsKey(splitString[0])) {
                        joinedFields.get(splitString[0]).add(splitString[1]);
                    } else {
                        joinedFields.putIfAbsent(splitString[0],
                                new HashSet<String>(Arrays.asList(splitString[1])));
                    }
                } else {
                    throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
                            "Could not parse field: " + jsonString.getString());
                }
            }
        }
    }

    /**
     * Parses a query and associated information from an incoming request without
     * any logic specific to a single index or entity. As such it may not be as
     * powerful, but is sufficient for simple queries (like those for faceting).
     * 
     * @param jsonQuery   Incoming query request encoded as Json.
     * @param luceneQuery Lucene BooleanQuery.Builder
     * @throws LuceneException If the types of the JsonValues in the query do not
     *                         match those supported by icat.lucene
     */
    private void parseGenericQuery(JsonObject jsonQuery) throws LuceneException {
        BooleanQuery.Builder luceneQuery = new BooleanQuery.Builder();
        for (Entry<String, JsonValue> entry : jsonQuery.entrySet()) {
            String field = entry.getKey();
            ValueType valueType = entry.getValue().getValueType();
            switch (valueType) {
                case STRING:
                    JsonString stringValue = (JsonString) entry.getValue();
                    String fld = lucene.facetFields.contains(field) ? field + ".keyword" : field;
                    luceneQuery.add(new TermQuery(new Term(fld, stringValue.getString())), Occur.MUST);
                    break;
                case NUMBER:
                    JsonNumber numberValue = (JsonNumber) entry.getValue();
                    if (DocumentMapping.longFields.contains(field)) {
                        luceneQuery.add(LongPoint.newExactQuery(field, numberValue.longValueExact()), Occur.FILTER);
                    } else if (DocumentMapping.doubleFields.contains(field)) {
                        luceneQuery.add(DoublePoint.newExactQuery(field, numberValue.doubleValue()), Occur.FILTER);
                    } else {
                        throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
                                "Value had type NUMBER, but field " + field
                                        + " is not a known longField or doubleField");
                    }
                    break;
                case ARRAY:
                    ArrayList<Long> longList = new ArrayList<>();
                    ArrayList<BytesRef> bytesRefList = new ArrayList<>();
                    JsonArray arrayValue = (JsonArray) entry.getValue();
                    for (JsonValue value : arrayValue) {
                        ValueType arrayValueType = value.getValueType();
                        switch (arrayValueType) {
                            case NUMBER:
                                longList.add(((JsonNumber) value).longValueExact());
                                break;
                            default:
                                bytesRefList.add(new BytesRef(((JsonString) value).getString()));
                                break;
                        }
                    }

                    if (longList.size() == 0 && bytesRefList.size() == 0) {
                        query = new MatchNoDocsQuery("Tried filtering" + field + " with an empty array");
                        return;
                    }
                    if (longList.size() != 0) {
                        luceneQuery.add(LongPoint.newSetQuery(field, longList), Occur.MUST);
                    }
                    if (bytesRefList.size() != 0) {
                        luceneQuery.add(new TermInSetQuery(field, bytesRefList), Occur.MUST);
                    }
                    break;
                default:
                    throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
                            "Query values should be ARRAY, STRING or NUMBER, but had value of type " + valueType);
            }
        }
        query = maybeEmptyQuery(luceneQuery);
    }

    /**
     * Parses query applying to a single parameter from incoming Json.
     * 
     * @param p JsonValue (JsonObject) representing a query against a single
     *          parameter.
     * @return BooleanQuery.Builder for a single parameter.
     * @throws LuceneException
     */
    private Builder parseParameter(JsonValue p) throws LuceneException {
        JsonObject parameter = (JsonObject) p;
        BooleanQuery.Builder paramQuery = new BooleanQuery.Builder();
        String pName = parameter.getString("name", null);
        if (pName != null) {
            paramQuery.add(new WildcardQuery(new Term("type.name.keyword", pName)), Occur.MUST);
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
            double pLowerNumericValue = parameter.getJsonNumber("lowerNumericValue").doubleValue();
            double pUpperNumericValue = parameter.getJsonNumber("upperNumericValue").doubleValue();
            paramQuery.add(DoublePoint.newRangeQuery("numericValue", pLowerNumericValue, pUpperNumericValue),
                    Occur.MUST);
        }
        return paramQuery;
    }

    /**
     * Parses a Lucene FieldDoc to be "searched after" from a String representation
     * of a JSON array. null if searchAfter was itself null or an empty String.
     * 
     * @param searchAfter String representation of a JSON object containing the
     *                    document id or "doc" (String), score ("float") in that
     *                    order.
     * @throws LuceneException If an entry in the fields array is not a STRING or
     *                         NUMBER
     */
    private void parseSearchAfter(String searchAfter) throws LuceneException {
        if (searchAfter == null || searchAfter.equals("")) {
            return;
        }
        SortField[] sortFields = sort.getSort();
        JsonReader reader = Json.createReader(new StringReader(searchAfter));
        JsonObject object = reader.readObject();
        // shardIndex and Lucene doc Id are always needed to determine tie breaks, even
        // if the field sort resulted in no ties in the first place
        int shardIndex = object.getInt("shardIndex");
        int doc = object.getInt("doc");
        float score = Float.NaN;
        List<Object> fields = new ArrayList<>();
        if (object.containsKey("score")) {
            score = object.getJsonNumber("score").bigDecimalValue().floatValue();
        }
        if (object.containsKey("fields")) {
            JsonArray jsonArray = object.getJsonArray("fields");
            if (jsonArray.size() != sortFields.length) {
                throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
                        "fields should have the same length as sort, but they were "
                                + jsonArray.size() + " and " + sortFields.length);
            }
            for (int i = 0; i < sortFields.length; i++) {
                JsonValue value = jsonArray.get(i);
                switch (value.getValueType()) {
                    case NUMBER:
                        JsonNumber number = ((JsonNumber) value);
                        switch (sortFields[i].getType()) {
                            case FLOAT:
                            case DOUBLE:
                            case SCORE:
                                fields.add(number.bigDecimalValue().floatValue());
                                break;
                            case INT:
                            case LONG:
                            case DOC:
                            case CUSTOM:
                                fields.add(number.longValueExact());
                                break;
                            default:
                                throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
                                        "fields contained a NUMBER but the corresponding field was "
                                                + sortFields[i]);
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
        this.searchAfter = new FieldDoc(doc, score, fields.toArray(), shardIndex);
    }

    /**
     * Parses the String from the request into a Lucene Sort object. Multiple sort
     * criteria are supported, and will be applied in order.
     * 
     * @param sortString String representation of a JSON object with the field(s) to
     *                   sort as keys, and the direction ("asc" or "desc") as value(s).
     * @throws LuceneException If the value for any key isn't "asc" or "desc"
     */
    public void parseSort(String sortString) throws LuceneException {
        if (sortString == null || sortString.equals("") || sortString.equals("{}")) {
            scored = true;
            sort = new Sort(SortField.FIELD_SCORE, new SortedNumericSortField("id", Type.LONG));
            return;
        }
        try (JsonReader reader = Json.createReader(new ByteArrayInputStream(sortString.getBytes()))) {
            JsonObject object = reader.readObject();
            List<SortField> fields = new ArrayList<>();
            for (String key : object.keySet()) {
                String order = object.getString(key);
                boolean reverse;
                if (order.equals("asc")) {
                    reverse = false;
                } else if (order.equals("desc")) {
                    reverse = true;
                } else {
                    throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
                            "Sort order must be 'asc' or 'desc' but it was '" + order + "'");
                }

                if (DocumentMapping.longFields.contains(key)) {
                    fields.add(new SortedNumericSortField(key, Type.LONG, reverse));
                } else if (DocumentMapping.doubleFields.contains(key)) {
                    fields.add(new SortedNumericSortField(key, Type.DOUBLE, reverse));
                } else {
                    fields.add(new SortField(key, Type.STRING, reverse));
                }
            }
            fields.add(new SortedNumericSortField("id", Type.LONG));
            scored = false;
            sort = new Sort(fields.toArray(new SortField[0]));
        }
    }
}
