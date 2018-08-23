package com.bigdata.demo.lucene.field;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

public class IntField extends Field {

    public IntField(String fieldName, int value, FieldType type) {
        super(fieldName, type);
        this.fieldsData = Integer.valueOf(value);
    }

    @Override
    public BytesRef binaryValue() {
        byte[] bytes = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes((Integer) this.fieldsData, bytes, 0);
        return new BytesRef(bytes);
    }
}
