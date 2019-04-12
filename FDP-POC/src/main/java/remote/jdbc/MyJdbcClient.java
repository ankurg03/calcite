package remote.jdbc;

import org.apache.calcite.avatica.remote.Driver;
import java.sql.*;

public class MyJdbcClient {

    public static void main(String[] args) {
        final int port = 9091;
        try {
            String url = "jdbc:avatica:remote:url=http://localhost:" + port
                    + ";serialization=" + Driver.Serialization.PROTOBUF.name();
            System.out.println(url);
            Connection connection =  DriverManager.getConnection(url);
            final String sql = "select * from STREAMS.ORDERS where product = 'paint'";
            final PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet rs = statement.executeQuery();
            printResult(rs);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
}
