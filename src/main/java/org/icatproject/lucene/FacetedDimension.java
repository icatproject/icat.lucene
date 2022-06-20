package org.icatproject.lucene;

import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObjectBuilder;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.Range;

/**
 * For a single dimension (field), stores labels (the unique values or ranges of
 * values for that field in the index) and their respective counts (the number
 * of times that label appears in different documents).
 * 
 * For example, a dimension might be "colour", the label "red", and the count 5.
 */
public class FacetedDimension {

	private String dimension;
	private List<Range> ranges;
	private List<String> labels;
	private List<Long> counts;

	/**
	 * Creates an "empty" FacetedDimension. The dimension (field) is set but ranges,
	 * labels and counts are not.
	 * 
	 * @param dimension The dimension, or field, to be faceted
	 */
	public FacetedDimension(String dimension) {
		this.dimension = dimension;
		this.ranges = new ArrayList<>();
		this.labels = new ArrayList<>();
		this.counts = new ArrayList<>();
	}

	/**
	 * Extracts the count for each label in the FacetResult. If the label has
	 * already been encountered, the count is incremented rather than being
	 * overridden. Essentially, this allows faceting to be performed across multiple
	 * shards.
	 * 
	 * @param facetResult A Lucene FacetResult object corresponding the relevant
	 *                    dimension
	 */
	public void addResult(FacetResult facetResult) {
		for (LabelAndValue labelAndValue : facetResult.labelValues) {
			String label = labelAndValue.label;
			int labelIndex = labels.indexOf(label);
			if (labelIndex == -1) {
				labels.add(label);
				counts.add(labelAndValue.value.longValue());
			} else {
				counts.set(labelIndex, counts.get(labelIndex) + labelAndValue.value.longValue());
			}
		}
	}

	/**
	 * Formats the labels and counts into Json.
	 * 
	 * @param aggregationsBuilder The JsonObjectBuilder to add the facets for this
	 *                            dimension to.
	 */
	public void buildResponse(JsonObjectBuilder aggregationsBuilder) {
		JsonObjectBuilder bucketsBuilder = Json.createObjectBuilder();
		for (int i = 0; i < labels.size(); i++) {
			JsonObjectBuilder bucketBuilder = Json.createObjectBuilder();
			bucketBuilder.add("doc_count", counts.get(i));
			if (ranges.size() > i) {
				Range range = ranges.get(i);
				if (range.getClass().getSimpleName().equals("LongRange")) {
					bucketBuilder.add("from", ((LongRange) range).min);
					bucketBuilder.add("to", ((LongRange) range).max);
				} else if (range.getClass().getSimpleName().equals("DoubleRange")) {
					bucketBuilder.add("from", ((DoubleRange) range).min);
					bucketBuilder.add("to", ((DoubleRange) range).max);
				}
			}
			bucketsBuilder.add(labels.get(i), bucketBuilder);
		}
		aggregationsBuilder.add(dimension, Json.createObjectBuilder().add("buckets", bucketsBuilder));
	}

	/**
	 * @return The list of Lucene Range Objects for use with numerical facets.
	 *         For String faceting, this will be empty.
	 */
	public List<Range> getRanges() {
		return ranges;
	}

	/**
	 * @return The dimension that these labels and counts correspond to.
	 */
	public String getDimension() {
		return dimension;
	}

}
