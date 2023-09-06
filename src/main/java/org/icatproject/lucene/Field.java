package org.icatproject.lucene;

import jakarta.json.JsonObject;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * Wrapper for the name, value and type (String/Text, long, double) of a field
 * to be added to a Lucene Document.
 */
class Field {

    private abstract class InnerField {

        public abstract void addSortable(Document document) throws NumberFormatException;

        public abstract void addToDocument(Document document) throws NumberFormatException;

    }

    private class InnerStringField extends InnerField {

        private String value;

        public InnerStringField(String value) {
            this.value = value;
        }

        @Override
        public void addSortable(Document document) throws NumberFormatException {
            if (DocumentMapping.sortFields.contains(name)) {
                if (name.equals("id")) {
                    // Id is a special case, as we need to to be SORTED as a byte ref to allow joins
                    // but also SORTED_NUMERIC to ensure a deterministic order to results
                    Long longValue = new Long(value);
                    document.add(new NumericDocValuesField("id.long", longValue));
                    document.add(new StoredField("id.long", longValue));
                    document.add(new LongPoint("id.long", longValue));
                }
                document.add(new SortedDocValuesField(name, new BytesRef(value)));
            }
        }

        @Override
        public void addToDocument(Document document) throws NumberFormatException {
            addSortable(document);

            if (DocumentMapping.facetFields.contains(name)) {
                document.add(new SortedSetDocValuesFacetField(name + ".keyword", value));
                document.add(new StringField(name + ".keyword", value, Store.NO));
            }

            if (DocumentMapping.textFields.contains(name)) {
                document.add(new TextField(name, value, Store.YES));
            } else {
                document.add(new StringField(name, value, Store.YES));
            }

        }

    }

    private class InnerLongField extends InnerField {

        private long value;

        public InnerLongField(long value) {
            this.value = value;
        }

        @Override
        public void addSortable(Document document) throws NumberFormatException {
            if (DocumentMapping.sortFields.contains(name)) {
                document.add(new NumericDocValuesField(name, value));
            }
        }

        @Override
        public void addToDocument(Document document) throws NumberFormatException {
            addSortable(document);
            document.add(new LongPoint(name, value));
            document.add(new StoredField(name, value));
        }

    }

    private class InnerDoubleField extends InnerField {

        private double value;

        public InnerDoubleField(double value) {
            this.value = value;
        }

        @Override
        public void addSortable(Document document) throws NumberFormatException {
            if (DocumentMapping.sortFields.contains(name)) {
                long sortableLong = NumericUtils.doubleToSortableLong(value);
                document.add(new NumericDocValuesField(name, sortableLong));
            }
        }

        @Override
        public void addToDocument(Document document) throws NumberFormatException {
            addSortable(document);
            document.add(new DoublePoint(name, value));
            document.add(new StoredField(name, value));
        }

    }

    private String name;
    private InnerField innerField;

    /**
     * Creates a wrapper for a Field.
     * 
     * @param object JsonObject containing representations of multiple fields
     * @param key    Key of a specific field in object
     */
    public Field(JsonObject object, String key) {
        name = key;
        if (DocumentMapping.doubleFields.contains(name)) {
            innerField = new InnerDoubleField(object.getJsonNumber(name).doubleValue());
        } else if (DocumentMapping.longFields.contains(name)) {
            innerField = new InnerLongField(object.getJsonNumber(name).longValueExact());
        } else {
            innerField = new InnerStringField(object.getString(name));
        }
    }

    /**
     * Creates a wrapper for a Field.
     * 
     * @param indexableField A Lucene IndexableField
     */
    public Field(IndexableField indexableField) {
        name = indexableField.name();
        if (DocumentMapping.doubleFields.contains(name)) {
            innerField = new InnerDoubleField(indexableField.numericValue().doubleValue());
        } else if (DocumentMapping.longFields.contains(name)) {
            innerField = new InnerLongField(indexableField.numericValue().longValue());
        } else {
            innerField = new InnerStringField(indexableField.stringValue());
        }
    }

    /**
     * Adds a sortable field to the passed document. This only accounts for sorting,
     * if storage and searchability are also needed, see {@link #addToDocument}. The
     * exact implementation depends on whether this is a String, long or double
     * field.
     * 
     * @param document The document to add to
     * @throws NumberFormatException
     */
    public void addSortable(Document document) throws NumberFormatException {
        innerField.addSortable(document);
    }

    /**
     * Adds this field to the passed document. This accounts for sortable and
     * facetable fields. The exact implementation depends on whether this is a
     * String, long or double field.
     * 
     * @param document The document to add to
     * @throws NumberFormatException
     */
    public void addToDocument(Document document) throws NumberFormatException {
        innerField.addToDocument(document);
    }

}
