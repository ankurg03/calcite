package schema.regestry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TestSchemaRegistry {

    static String SCHEMA = "{\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"fieldName\": \"id\",\n" +
            "      \"fieldSchema\": {\n" +
            "        \"fieldType\": \"INT32\",\n" +
            "        \"rowSchema\": null,\n" +
            "        \"elementSchema\": null,\n" +
            "        \"valueScehma\": null,\n" +
            "        \"primitiveField\": true\n" +
            "      },\n" +
            "      \"position\": 0\n" +
            "    },\n" +
            "    {\n" +
            "      \"fieldName\": \"name\",\n" +
            "      \"fieldSchema\": {\n" +
            "        \"fieldType\": \"STRING\",\n" +
            "        \"rowSchema\": null,\n" +
            "        \"elementSchema\": null,\n" +
            "        \"valueScehma\": null,\n" +
            "        \"primitiveField\": true\n" +
            "      },\n" +
            "      \"position\": 1\n" +
            "    },\n" +
            "    {\n" +
            "      \"fieldName\": \"companyId\",\n" +
            "      \"fieldSchema\": {\n" +
            "        \"fieldType\": \"INT32\",\n" +
            "        \"rowSchema\": null,\n" +
            "        \"elementSchema\": null,\n" +
            "        \"valueScehma\": null,\n" +
            "        \"primitiveField\": true\n" +
            "      },\n" +
            "      \"position\": 2\n" +
            "    },\n" +
            "    {\n" +
            "      \"fieldName\": \"address\",\n" +
            "      \"fieldSchema\": {\n" +
            "        \"fieldType\": \"ROW\",\n" +
            "        \"rowSchema\": {\n" +
            "          \"fields\": [\n" +
            "            {\n" +
            "              \"fieldName\": \"zip\",\n" +
            "              \"fieldSchema\": {\n" +
            "                \"fieldType\": \"INT32\",\n" +
            "                \"rowSchema\": null,\n" +
            "                \"elementSchema\": null,\n" +
            "                \"valueScehma\": null,\n" +
            "                \"primitiveField\": true\n" +
            "              },\n" +
            "              \"position\": 0\n" +
            "            },\n" +
            "            {\n" +
            "              \"fieldName\": \"streetnum\",\n" +
            "              \"fieldSchema\": {\n" +
            "                \"fieldType\": \"ROW\",\n" +
            "                \"rowSchema\": {\n" +
            "                  \"fields\": [\n" +
            "                    {\n" +
            "                      \"fieldName\": \"number\",\n" +
            "                      \"fieldSchema\": {\n" +
            "                        \"fieldType\": \"INT32\",\n" +
            "                        \"rowSchema\": null,\n" +
            "                        \"elementSchema\": null,\n" +
            "                        \"valueScehma\": null,\n" +
            "                        \"primitiveField\": true\n" +
            "                      },\n" +
            "                      \"position\": 0\n" +
            "                    }\n" +
            "                  ]\n" +
            "                },\n" +
            "                \"elementSchema\": null,\n" +
            "                \"valueScehma\": null,\n" +
            "                \"primitiveField\": false\n" +
            "              },\n" +
            "              \"position\": 1\n" +
            "            }\n" +
            "          ]\n" +
            "        },\n" +
            "        \"elementSchema\": null,\n" +
            "        \"valueScehma\": null,\n" +
            "        \"primitiveField\": false\n" +
            "      },\n" +
            "      \"position\": 3\n" +
            "    },\n" +
            "    {\n" +
            "      \"fieldName\": \"selfEmployed\",\n" +
            "      \"fieldSchema\": {\n" +
            "        \"fieldType\": \"BOOLEAN\",\n" +
            "        \"rowSchema\": null,\n" +
            "        \"elementSchema\": null,\n" +
            "        \"valueScehma\": null,\n" +
            "        \"primitiveField\": true\n" +
            "      },\n" +
            "      \"position\": 4\n" +
            "    },\n" +
            "    {\n" +
            "      \"fieldName\": \"phoneNumbers\",\n" +
            "      \"fieldSchema\": {\n" +
            "        \"fieldType\": \"ARRAY\",\n" +
            "        \"rowSchema\": null,\n" +
            "        \"elementSchema\": {\n" +
            "          \"fieldType\": \"ROW\",\n" +
            "          \"rowSchema\": {\n" +
            "            \"fields\": [\n" +
            "              {\n" +
            "                \"fieldName\": \"kind\",\n" +
            "                \"fieldSchema\": {\n" +
            "                  \"fieldType\": \"STRING\",\n" +
            "                  \"rowSchema\": null,\n" +
            "                  \"elementSchema\": null,\n" +
            "                  \"valueScehma\": null,\n" +
            "                  \"primitiveField\": true\n" +
            "                },\n" +
            "                \"position\": 0\n" +
            "              },\n" +
            "              {\n" +
            "                \"fieldName\": \"number\",\n" +
            "                \"fieldSchema\": {\n" +
            "                  \"fieldType\": \"STRING\",\n" +
            "                  \"rowSchema\": null,\n" +
            "                  \"elementSchema\": null,\n" +
            "                  \"valueScehma\": null,\n" +
            "                  \"primitiveField\": true\n" +
            "                },\n" +
            "                \"position\": 1\n" +
            "              }\n" +
            "            ]\n" +
            "          },\n" +
            "          \"elementSchema\": null,\n" +
            "          \"valueScehma\": null,\n" +
            "          \"primitiveField\": false\n" +
            "        },\n" +
            "        \"valueScehma\": null,\n" +
            "        \"primitiveField\": false\n" +
            "      },\n" +
            "      \"position\": 5\n" +
            "    },\n" +
            "    {\n" +
            "      \"fieldName\": \"mapValues\",\n" +
            "      \"fieldSchema\": {\n" +
            "        \"fieldType\": \"MAP\",\n" +
            "        \"rowSchema\": null,\n" +
            "        \"elementSchema\": null,\n" +
            "        \"valueScehma\": {\n" +
            "          \"fieldType\": \"ANY\",\n" +
            "          \"rowSchema\": null,\n" +
            "          \"elementSchema\": null,\n" +
            "          \"valueScehma\": null,\n" +
            "          \"primitiveField\": true\n" +
            "        },\n" +
            "        \"primitiveField\": false\n" +
            "      },\n" +
            "      \"position\": 6\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    static String threeLevelNested = "{\"fields\":[{\"fieldName\":\"address\",\"fieldSchema\":{\"fieldType\":\"ROW\",\"rowSchema\":{\"fields\":[{\"fieldName\":\"zip\",\"fieldSchema\":{\"fieldType\":\"INT32\",\"rowSchema\":null,\"elementSchema\":null,\"valueScehma\":null,\"primitiveField\":true},\"position\":0},{\"fieldName\":\"streetnum\",\"fieldSchema\":{\"fieldType\":\"ROW\",\"rowSchema\":{\"fields\":[{\"fieldName\":\"number\",\"fieldSchema\":{\"fieldType\":\"ROW\",\"rowSchema\":{\"fields\":[{\"fieldName\":\"innernumber\",\"fieldSchema\":{\"fieldType\":\"ROW\",\"rowSchema\":{\"fields\":[{\"fieldName\":\"lowestnumber\",\"fieldSchema\":{\"fieldType\":\"INT32\",\"rowSchema\":null,\"elementSchema\":null,\"valueScehma\":null,\"primitiveField\":true},\"position\":0}]},\"elementSchema\":null,\"valueScehma\":null,\"primitiveField\":true},\"position\":0}]},\"elementSchema\":null,\"valueScehma\":null,\"primitiveField\":true},\"position\":0}]},\"elementSchema\":null,\"valueScehma\":null,\"primitiveField\":false},\"position\":1}]},\"elementSchema\":null,\"valueScehma\":null,\"primitiveField\":false},\"position\":0}]}";


    private static Table createTableFromRelSchema(RelDataType relationalSchema) {
        return new AbstractTable() {
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return relationalSchema;
            }
        };
    }

    public static void main(String[] args) throws IOException, SQLException, ValidationException, RelConversionException, SqlParseException {
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println(threeLevelNested);

        JsonNode node = objectMapper.readTree(threeLevelNested);


        SQLSchema sqlSchema = objectMapper.treeToValue(node, SQLSchema.class);


        RelSchemaConverter relSchemaConverter = new RelSchemaConverter();
        RelDataType relationalSchema = relSchemaConverter.convertToRelSchema(sqlSchema);




        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        System.out.println(relationalSchema);

        rootSchema.add("empDb", new AbstractSchema());

        rootSchema.getSubSchema("empDb").add("employInfo", createTableFromRelSchema(relationalSchema));

//        rootSchema.getSubSchema("empDb").getSubSchema("employInfo").add("address", createTableFromRelSchema(addrrelationalSchema));

        final List<RelTraitDef> traitDefs = new ArrayList<>();

        traitDefs.add(ConventionTraitDef.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);
        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder().setLex(Lex.JAVA).build())
                .defaultSchema(rootSchema)
//                .operatorTable(new ChainedSqlOperatorTable(sqlOperatorTables))
                .traitDefs(traitDefs)
                .context(Contexts.EMPTY_CONTEXT)
                .costFactory(null)
                .build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);

        System.out.println("test....");
        /**
         * query for SCHEMA
         * SqlNode sql = planner.parse("select * from empDb.employInfo e, empDb.employInfo es where e.address.zip=es.address.streetnum.number");
         **/
        SqlNode sql = planner.parse("select * from empDb.employInfo e, empDb.employInfo es where e.address.zip=es.address.streetnum.number.innernumber.lowestnumber");

        SqlNode validatedSql = planner.validate(sql);
        RelRoot relRoot = planner.rel(validatedSql);
        System.out.println(RelOptUtil.toString(relRoot.project()));
    }
}
