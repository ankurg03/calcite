package schema.regestry;

import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RelSchemaConverter extends SqlTypeFactoryImpl {

    private static final Logger LOG = LoggerFactory.getLogger(RelSchemaConverter.class);

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
            case ANY:
                // TODO Calcite execution engine doesn't support record type yet.
                return createTypeWithNullability(createSqlType(SqlTypeName.ANY), true);
            case MAP:
                RelDataType valueType = getRelDataType(fieldSchema.getValueScehma());
                return super.createMapType(createTypeWithNullability(createSqlType(SqlTypeName.VARCHAR), true),
                        createTypeWithNullability(valueType, true));
            default:
                String msg = String.format("Field Type %s is not supported", fieldSchema.getFieldType());
                LOG.error(msg);
                throw new RuntimeException(msg);
        }
    }
}
