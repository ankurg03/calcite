package com.flipkart.fsql.schema.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown=true)
public class SqlFieldSchema {
    private FSqlFieldType fieldType;
    private SqlFieldSchema elementType;
    private SqlFieldSchema valueType;
    private SQLSchema rowSchema;

    @JsonCreator
    private SqlFieldSchema(@JsonProperty("fieldType") FSqlFieldType fieldType,
                           @JsonProperty("elementSchema") SqlFieldSchema elementType,
                           @JsonProperty("valueScehma") SqlFieldSchema valueType,
                           @JsonProperty("rowSchema") SQLSchema rowSchema) {
        this.fieldType = fieldType;
        this.elementType = elementType;
        this.valueType = valueType;
        this.rowSchema = rowSchema;
    }

    /**
     * Create a primitive fi
     * @param typeName
     * @return
     */
    public static SqlFieldSchema createPrimitiveSchema(FSqlFieldType typeName) {
        return new SqlFieldSchema(typeName, null, null, null);
    }

    public static SqlFieldSchema createArraySchema(SqlFieldSchema elementType) {
        return new SqlFieldSchema(FSqlFieldType.ARRAY, elementType, null, null);
    }

    public static SqlFieldSchema createMapSchema(SqlFieldSchema valueType) {
        return new SqlFieldSchema(FSqlFieldType.MAP, null, valueType, null);
    }

    public static SqlFieldSchema createRowFieldSchema(SQLSchema rowSchema) {
        return new SqlFieldSchema(FSqlFieldType.ROW, null, null, rowSchema);
    }

    /**
     * @return whether the field is a primitive field type or not.
     */
    public boolean isPrimitiveField() {
        return fieldType != FSqlFieldType.ARRAY && fieldType != FSqlFieldType.MAP && fieldType != FSqlFieldType.ROW;
    }

    /**
     * Get teh Type of the Samza SQL Field.
     * @return
     */
    public FSqlFieldType getFieldType() {
        return fieldType;
    }

    /**
     * Get the element schema if the field type is {@link FSqlFieldType#ARRAY}
     */
    public SqlFieldSchema getElementSchema() {
        return elementType;
    }

    /**
     * Get the schema of the value if the field type is {@link FSqlFieldType#MAP}
     */
    public SqlFieldSchema getValueScehma() {
        return valueType;
    }

    /**
     * Get the row schema if the field type is {@link FSqlFieldType#ROW}
     */
    public SQLSchema getRowSchema() {
        return rowSchema;
    }
    
}
