package igniteStartup;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Program {
    IgniteConnection connection;
    Ignite ignite;

    Program() {
        ignite = Ignition.start();
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
        data.insertCities(ignite);
    }

    private void streamData() throws SQLException, ClassNotFoundException {
        connection.establish();
        Data data = new Data();
        data.streamInData(ignite);
    }

    private void putCities() throws SQLException, ClassNotFoundException {
        connection.establish();
        Data data = new Data();
        data.putCities(ignite);
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

    private void queryCacheCities() throws SQLException, ClassNotFoundException {
        connection.establish();
        Data data = new Data();
        data.queryCacheCities(ignite);
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        Program program = new Program();
        System.out.println("options:\n 1 - build structure\n2 - query cities\n3 - insert cities\n4 - stream in cities\n5 - put cities\n6 - query cache cities\nq - quit");
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
                case '4':
                    System.out.println("stream in cities");
                    program.streamData();
                    break;
                case '5':
                    System.out.println("put cities");
                    program.putCities();
                    break;
                case '6':
                    System.out.println("query cache cities");
                    program.queryCacheCities();
                    break;
                default:
                    break;
            }
        }
        System.out.println("quit");
    }
}
