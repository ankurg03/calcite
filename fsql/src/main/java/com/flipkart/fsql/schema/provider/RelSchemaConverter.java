package com.flipkart.fsql.schema.provider;

import com.flipkart.fsql.schema.model.SQLSchema;
import com.flipkart.fsql.schema.model.SqlFieldSchema;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RelSchemaConverter extends SqlTypeFactoryImpl {

    public RelSchemaConverter() {
        super(RelDataTypeSystem.DEFAULT);
    }

    public RelDataType convertToRelSchema(SQLSchema SQLSchema) {
        return convertRecordType(SQLSchema);
    }

    private RelDataType convertRecordType(SQLSchema schema) {
        List<RelDataTypeField> relFields = getRelFields(schema.getFields());
        return new RelRecordType(relFields);
    }

    private List<RelDataTypeField> getRelFields(List<SQLSchema.SqlField> fields) {
        List<RelDataTypeField> relFields = new ArrayList<>();

        for (SQLSchema.SqlField field : fields) {
            String fieldName = field.getFieldName();
            int fieldPos = field.getPosition();
            RelDataType dataType = getRelDataType(field.getFieldSchema());
            relFields.add(new RelDataTypeFieldImpl(fieldName, fieldPos, dataType));
        }

        return relFields;
    }

    private RelDataType getRelDataType(SqlFieldSchema fieldSchema) {
        switch (fieldSchema.getFieldType()) {
            case ARRAY:
                RelDataType elementType = getRelDataType(fieldSchema.getElementSchema());
                return new ArraySqlType(elementType, true);
            case BOOLEAN:
                return createTypeWithNullability(createSqlType(SqlTypeName.BOOLEAN), true);
            case DOUBLE:
                return createTypeWithNullability(createSqlType(SqlTypeName.DOUBLE), true);
            case FLOAT:
                return createTypeWithNullability(createSqlType(SqlTypeName.FLOAT), true);
            case STRING:
                return createTypeWithNullability(createSqlType(SqlTypeName.VARCHAR), true);
            case BYTES:
                return createTypeWithNullability(createSqlType(SqlTypeName.VARBINARY), true);
            case INT16:
            case INT32:
                return createTypeWithNullability(createSqlType(SqlTypeName.INTEGER), true);
            case INT64:
                return createTypeWithNullability(createSqlType(SqlTypeName.BIGINT), true);
            case ROW:
                ArrayList<RelDataType> relDataTypes = new ArrayList<>();
                ArrayList<String> relNames = new ArrayList<>();
                for (SQLSchema.SqlField field : fieldSchema.getRowSchema().getFields()) {
                    String fieldName = field.getFieldName();
                    RelDataType dataType = getRelDataType(field.getFieldSchema());
                    relDataTypes.add(dataType);
                    relNames.add(fieldName);
                }
                return createStructType(relDataTypes, relNames);
            case ANY:
                return createTypeWithNullability(createSqlType(SqlTypeName.ANY), true);
            case MAP:
                RelDataType valueType = getRelDataType(fieldSchema.getValueScehma());
                return super.createMapType(createTypeWithNullability(createSqlType(SqlTypeName.VARCHAR), true),
                        createTypeWithNullability(valueType, true));
            default:
                String msg = String.format("Field Type %s is not supported", fieldSchema.getFieldType());
                throw new RuntimeException(msg);
        }
    }
}
