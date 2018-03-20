package igniteStartup;


import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
//import org.slf4j.Logger;

public class Data {
    //	final Logger logger = LoggerFactory.getLogger(Wombat.class);
    private final IgniteConfiguration igniteConfiguration = new IgniteConfiguration();


    public Data() {
        CacheConfiguration cacheCfg = new CacheConfiguration("myCache");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        igniteConfiguration.setCacheConfiguration(cacheCfg);
    }

    public void insertCities(Ignite ignite) {
        // Connecting to the cluster.
//		Ignite ignite = Ignition.getOrStart()
//		Ignite ignite = Ignition.ignite();

        // Getting a reference to an underlying cache created for City table above.
        IgniteCache<Long, City> cityCache = ignite.getOrCreateCache(igniteConfiguration) .cache("SQL_PUBLIC_CITY");

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

    public void putCities(Ignite ignite) {
        final City city1 = new City();
        city1.setId(4L);
        city1.setName("Tarnow");

        try (IgniteCache<Long, City> cityCache = ignite.getOrCreateCache("SQL_PUBLIC_CITY")) {
            for (long i = 4L; i <= 10; i++) {
                city1.setId(i);
                cityCache.put(city1.getId(), city1);
            }
        }
    }

    public void queryCacheCities(Ignite ignite) {
        try (IgniteCache<Long, City> cityCache = ignite.cache("SQL_PUBLIC_CITY")) {
            for (int i = 0; i < 10; i++)
                System.out.println("Got [key=" + i + ", city=" + cityCache.get((long) i) + ']');
            City c4 = cityCache.get(4L);
            System.out.println(c4);
        }
    }

    public void queryCities(IgniteConnection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT c.name FROM SQL_PUBLIC_CITY c ")) {

                System.out.println("Query result:");

                while (rs.next())
                    System.out.println(">>>    " + rs.getString(1));
            }
        }
    }

    public void streamInData(Ignite ignite) {
        final City city1 = new City();
        city1.setId(5L);
        city1.setName("Gdansk");

        try (IgniteCache<Long, City> ignored = ignite.getOrCreateCache("SQL_PUBLIC_CITY")) {
            try (IgniteDataStreamer<Object, Object> dataStreamer = ignite.dataStreamer("SQL_PUBLIC_CITY")) {
//                dataStreamer.allowOverwrite(true);
                // Stream entries.
                for (int i = 0; i < 100000; i++) {
                    city1.setId((long) i);
                    dataStreamer.addData(city1.getId(), city1);
                }
            }
        }
    }
}
