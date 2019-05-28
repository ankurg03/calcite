package schema.regestry;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@JsonIgnoreProperties(ignoreUnknown=true)
public class SQLSchema {
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class SqlField {


        private String fieldName;


        private SqlFieldSchema fieldSchema;


        private int position;

        @JsonCreator
        public SqlField(@JsonProperty("position") int position, @JsonProperty("filedName") String fieldName, @JsonProperty("fieldSchema") SqlFieldSchema fieldSchema) {
            this.position = position;
            this.fieldName = fieldName;
            this.fieldSchema = fieldSchema;
        }

        public int getPosition() {
            return position;
        }

        public void setPosition(int position) {
            this.position = position;
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public SqlFieldSchema getFieldSchema() {
            return fieldSchema;
        }

        public void setFieldSchema(SqlFieldSchema fieldSchema) {
            this.fieldSchema = fieldSchema;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(SQLSchema.class);

//    @JsonProperty("fields")
    private List<SqlField> fields;

    public SQLSchema(List<String> colNames, List<SqlFieldSchema> colTypes) {
        if (colNames == null || colTypes == null || colNames.size() != colTypes.size()) {
            throw new IllegalArgumentException();
        }

        fields = IntStream.range(0, colTypes.size())
                .mapToObj(i -> new SqlField(i, colNames.get(i), colTypes.get(i)))
                .collect(Collectors.toList());
    }

    @JsonCreator
    public SQLSchema(@JsonProperty("fields") List<SqlField> fields) {
        this.fields = fields;
    }

    public boolean containsField(String keyName) {
        return fields.stream().anyMatch(x -> x.getFieldName().equals(keyName));
    }


    public List<SqlField> getFields() {
        return fields;
    }
}