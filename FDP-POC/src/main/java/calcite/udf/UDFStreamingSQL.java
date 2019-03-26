package calcite.udf;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UDFStreamingSQL {
    public static final String STREAM_MODEL = "{\n" +
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
            "         factory: 'calcite.udf.UDFStreamingSQL$TestStreamTableFactory'\n" +
            "       } ]\n" +
            "     }   ]\n" +
            "}";

    public static void main(String[] args) throws SQLException {

        try (Connection connection =
                     DriverManager.getConnection("jdbc:calcite:model=inline:" + STREAM_MODEL)) {
            final CalciteConnection calciteConnection = connection.unwrap(
                    CalciteConnection.class);

            streamBasicTest(calciteConnection);
            streamScallerTest(calciteConnection);
        }
    }


    private static void streamScallerTest(CalciteConnection calciteConnection) throws SQLException {
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        SchemaPlus streamSchema = rootSchema.getSubSchema("com.org.namespace.name");


        streamSchema.add("SQUARE_TEST",
                ScalarFunctionImpl.create(SquareUDF.class, "eval"));

        String sql = "select stream STREAMS.SQUARE_TEST(UNITS) AS SQR_OF_UNIT from STREAMS.ORDERS";

        final PreparedStatement statement =
                calciteConnection.prepareStatement(sql);

        ResultSet rs = statement.executeQuery();
        printResult(rs);
    }

    private static void streamBasicTest(CalciteConnection calciteConnection) throws SQLException {
        String sql = "select stream * from STREAMS.ORDERS";

        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        SchemaPlus streamSchema = rootSchema.getSubSchema("STREAMS");
        UDFStreamingSQL.TestStreamTable t = (UDFStreamingSQL.TestStreamTable) streamSchema.getTable("ORDERS");
        t.add(6);
        t.add(7);
        final PreparedStatement statement =
                calciteConnection.prepareStatement(sql);
        t.add(8);
        ResultSet rs = statement.executeQuery();
        t.add(9);
        printResult(rs);
    }

    private static void printResult(ResultSet rs) throws SQLException {

        for(int i = 1 ; i <=  rs.getMetaData().getColumnCount(); i++) {
            System.out.print(rs.getMetaData().getColumnLabel(i) + "  ");
        }
        System.out.println();

        while (rs.next()) {
            for(int i = 1 ; i <=  rs.getMetaData().getColumnCount(); i++) {
                System.out.print(rs.getString(i) + "  ");
            }
            System.out.println();
        }
        System.out.println();
    }

    public static class SquareUDF {
        public long eval(int x) {
            return x * x;
        }
    }

    /** Mock table that returns a stream of orders from a fixed array. */
    @SuppressWarnings("UnusedDeclaration")
    public static class TestStreamTableFactory implements TableFactory<Table> {
        // public constructor, per factory contract
        public TestStreamTableFactory() {
        }

        public Table create(SchemaPlus schema, String name,
                            Map<String, Object> operand, RelDataType rowType) {
            return new UDFStreamingSQL.TestStreamTable(getRowList());
        }

        public static ImmutableList<Object[]> getRowList() {
            final Object[][] rows = {
                    {ts(10, 15, 0), 1, "paint", 10},
                    {ts(10, 24, 15), 2, "paper", 5},
                    {ts(10, 24, 45), 3, "brush", 12},
                    {ts(10, 58, 0), 4, "paint", 3},
                    {ts(11, 10, 0), 5, "paint", 3}
            };
            return ImmutableList.copyOf(rows);
        }

        private static Object ts(int h, int m, int s) {
            return DateTimeUtils.unixTimestamp(2015, 2, 15, h, m, s);
        }
    }

    public static class TestStreamTable implements ScannableTable, StreamableTable {

        private ArrayList<Object[]> rows = new ArrayList<>();


        public TestStreamTable(List<Object[]> rows) {
            this.rows.addAll(rows);
        }

        public void add(int id) {
            Object [] row = {11111, id, "paint", 10};
            rows.add(row);
        }

        protected final RelProtoDataType protoRowType = a0 -> a0.builder()
                .add("ROWTIME", SqlTypeName.TIMESTAMP)
                .add("ID", SqlTypeName.INTEGER)
                .add("PRODUCT", SqlTypeName.VARCHAR, 10)
                .add("UNITS", SqlTypeName.INTEGER)
                .build();

        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return protoRowType.apply(typeFactory);
        }

        public Statistic getStatistic() {
            return Statistics.of(100d, ImmutableList.of(),
                    RelCollations.createSingleton(0));
        }

        public Schema.TableType getJdbcTableType() {
            return Schema.TableType.TABLE;
        }

        public Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(rows);
        }

        @Override public Table stream() {
            return new UDFStreamingSQL.TestStreamTable(rows);
        }

        @Override public boolean isRolledUp(String column) {
            return false;
        }

        @Override public boolean rolledUpColumnValidInsideAgg(String column,
                                                              SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
            return false;
        }
    }

}
