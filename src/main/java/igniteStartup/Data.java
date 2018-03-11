package igniteStartup;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;

public class Data {
	public void insertCities() {
		// Connecting to the cluster.
		Ignite ignite = Ignition.start();
//		Ignite ignite = Ignition.ignite();
		
		// Getting a reference to an underlying cache created for City table above.
		IgniteCache<Long, City> cityCache = ignite.cache("SQL_PUBLIC_CITY");


//		// Inserting entries into City.
		SqlFieldsQuery query = new SqlFieldsQuery("INSERT INTO City (id, name) VALUES (?, ?)");

		cityCache.query(query.setArgs(1, "Forest Hill")).getAll();
		cityCache.query(query.setArgs(2, "Denver")).getAll();
		cityCache.query(query.setArgs(3, "St. Petersburg")).getAll();

		// Getting a reference to an underlying cache created for Person table above.
//		IgniteCache<PersonKey, Person> personCache = ignite.cache("SQL_PUBLIC_PERSON");
//
//		// Inserting entries into Person.
//		query = new SqlFieldsQuery("INSERT INTO Person (id, name, city_id) VALUES (?, ?, ?)");
//
//		personCache.query(query.setArgs(1, "John Doe", 3)).getAll();
//		personCache.query(query.setArgs(2, "Jane Roe", 2)).getAll();
//		personCache.query(query.setArgs(3, "Mary Major", 1)).getAll();
//		personCache.query(query.setArgs(4, "Richard Miles", 2)).getAll();
	}
	
	public void queryCities(IgniteConnection connection) throws SQLException {
		try (Statement stmt = connection.createStatement()) {
		    try (ResultSet rs =
		    stmt.executeQuery("SELECT c.name " +
		    " FROM City c " )) {

		      System.out.println("Query result:");

		      while (rs.next())
		         System.out.println(">>>    " + rs.getString(1));
		    }
		}
	}
}
