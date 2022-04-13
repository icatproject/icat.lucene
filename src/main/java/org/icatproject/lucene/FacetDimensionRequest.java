package org.icatproject.lucene;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.range.Range;

public class FacetDimensionRequest {

	private String dimension;
    private List<Range> ranges;

	public FacetDimensionRequest(String dimension) {
		this.dimension = dimension;
        this.ranges = new ArrayList<>();
	}

	public List<Range> getRanges() {
        return ranges;
    }

    public String getDimension() {
		return dimension;
	}

}
