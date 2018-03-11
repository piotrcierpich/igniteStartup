package igniteStartup;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Program {
	IgniteConnection connection;

	Program() {
		connection = new IgniteConnection();
	}

	private void buildStructure() throws ClassNotFoundException, SQLException {
		connection.establish();
		Structure structure = new Structure();
		structure.create(connection);
	}

	private void insertCities() throws ClassNotFoundException, SQLException {
		connection.establish();
		Data data = new Data();
		data.insertCities();
	}

	private void queryCities() throws ClassNotFoundException, SQLException {
		connection.establish();
		Data data = new Data();
		try {
			data.queryCities(connection);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
		Program program = new Program();
		System.out.println("options: 1 - build structure\n2 - query cities\n3 - insert cities\nq - quit");
		int optionRead;
		while ((optionRead = System.in.read()) != 'q') {
			switch (optionRead) {
			case '1':
				System.out.println("building structure");
				program.buildStructure();
				break;
			case '2':
				System.out.println("query cities");
				program.queryCities();
				break;
			case '3':
				System.out.println("insert cities");
				program.insertCities();
				break;
			default:
				break;
			}
		}
		System.out.println("quit");
	}
}
