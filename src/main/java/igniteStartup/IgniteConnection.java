package igniteStartup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class IgniteConnection {
	private Connection conn;

	public void establish() throws ClassNotFoundException, SQLException {
		if(conn != null)
			return;
		
		// Register JDBC driver
		Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
		// Open JDBC connection
		conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
	}

	public Statement createStatement() throws SQLException {
		return conn.createStatement();
	}
}
