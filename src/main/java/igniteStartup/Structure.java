package igniteStartup;

import java.sql.SQLException;
import java.sql.Statement;

public class Structure {
	public void create(IgniteConnection connection) throws SQLException {
		// Create database tables.
		Statement stmt = connection.createStatement();

		// Create table based on REPLICATED template.
		stmt.executeUpdate(
				"CREATE TABLE City (" + " id LONG PRIMARY KEY, name VARCHAR) " + " WITH \"template=replicated\"");

		// Create table based on PARTITIONED template with one backup.
		stmt.executeUpdate("CREATE TABLE Person (" + " id LONG, name VARCHAR, city_id LONG, "
				+ " PRIMARY KEY (id, city_id)) " + " WITH \"backups=1, affinityKey=city_id\"");

		// Create an index on the City table.
		stmt.executeUpdate("CREATE INDEX idx_city_name ON City (name)");

		// Create an index on the Person table.
		stmt.executeUpdate("CREATE INDEX idx_person_name ON Person (name)");
	}
}
