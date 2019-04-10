package planner.examples;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SamplePlanner {

    private final Planner planner;

    private static final String STREAM_MODEL = "{\n" +
            "  version: '1.0',\n" +
            "  defaultSchema: 'TEST',\n" +
            "   schemas: [\n" +
            "     {\n" +
            "       name: 'STREAMS',\n" +
            "       tables: [ {\n" +
            "         type: 'custom',\n" +
            "         name: 'ORDERS',\n" +
            "         stream: {\n" +
            "           stream: true\n" +
            "         },\n" +
            "         factory: 'planner.examples.OrderTableFactory'\n" +
            "       } ]\n" +
            "     }   ]\n" +
            "}";

    public SamplePlanner(SchemaPlus schema) {
        final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

        traitDefs.add(ConventionTraitDef.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);

        FrameworkConfig calciteFrameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                        .setLex(Lex.JAVA)
                        .build())
                // Sets the schema to use by the planner
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                // Context provides a way to store data within the planner session that can be accessed in planner rules.
                .context(Contexts.EMPTY_CONTEXT)
                // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
                .ruleSets(RuleSets.ofList())
                // Custom cost factory to use during optimization
                .costFactory(null)
                .typeSystem(RelDataTypeSystem.DEFAULT)
                .build();

        this.planner = Frameworks.getPlanner(calciteFrameworkConfig);
    }

    public RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException {
        SqlNode sqlNode;

        try {
            sqlNode = planner.parse(query);
        } catch (SqlParseException e) {
            throw new RuntimeException("Query parsing error.", e);
        }

        SqlNode validatedSqlNode = planner.validate(sqlNode);

        return planner.rel(validatedSqlNode).project();
    }

/*
    Steps to create the Calcite query planner.
    You have to supply

    configurations related to parser
    the default schema object to use
    trait definitions
    query planner rules
    query planning context to store data within the planner session
    cost factory
    type system to use

    To make it easier to create a planner, Calcite implements FrameworkConfig and Frameworks classes.
    As shown below, you can use FrameworkConfig and Frameworks classes to create a query planner object.
    This example generates logical plans from the given queries.
    here I am using an empty planner context, empty rule set and null cost factory when creating a the planner.

    Once you create the planner object as shown in the code, you can use it to parse, validate and generate the logical plan for SQL queries as shown in the getLogicalPlan method.
*/

    public static void main(String[] args) {
        final Properties properties = new Properties();
        try (Connection connection =
                        DriverManager.getConnection("jdbc:calcite:", properties)) {
            final CalciteConnection calciteConnection = connection.unwrap(
                    CalciteConnection.class);
            // ModelHandler reads the schema and load the schema to connection's root schema and sets the default schema
            new ModelHandler(calciteConnection, "inline:" + STREAM_MODEL);

            SchemaPlus rootSchema = calciteConnection.getRootSchema();

            SamplePlanner queryPlanner = new SamplePlanner(rootSchema);
            testSimpleQuery(queryPlanner);


        } catch (SQLException e) {
            e.printStackTrace();
        } catch (RelConversionException e) {
            e.printStackTrace();
        } catch (ValidationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void testSimpleQuery(SamplePlanner queryPlanner) throws ValidationException, RelConversionException {
        final String sql = "select id, product from Streams.ORDERS where product = 'paint'";
        RelNode loginalPlan = queryPlanner.getLogicalPlan(sql);
        System.out.println(RelOptUtil.toString(loginalPlan));
        /**
         * LogicalProject(id=[$0], product=[$1])
         *   LogicalFilter(condition=[=($1, 'paint')])
         *     LogicalTableScan(table=[[STREAMS, ORDERS]])
         */
    }
}
