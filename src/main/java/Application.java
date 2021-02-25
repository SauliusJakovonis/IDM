import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class Application {

    public static Connection getConnection() throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/imdb_100k";
        Properties props = new Properties();
        props.setProperty("user","postgres");
        props.setProperty("password","database");
        props.setProperty("ssl","false");
        return DriverManager.getConnection(url, props);
    }

    //Exercise 1 - Return all titles for a specific year.
    public static ArrayList<String> getAllTitles(int year) throws SQLException {
        ArrayList<String> ret = new ArrayList<>();
        Connection conn = getConnection();

        String sql = "SELECT title FROM title_100k WHERE production_year = ?";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, year);
        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            ret.add(rs.getString(1));
        }

        return ret;
    }

    //Exercise 3 - Return all titles that are associated with a specific keyword.
    public static ArrayList<String> getAllTitles(String keyword) throws SQLException {
        ArrayList<String> ret = new ArrayList<>();
        Connection conn = getConnection();

        String sql = "SELECT movies.title FROM title_100k AS movies" +
                " JOIN movie_keyword_100k AS k ON movies.id = k.movie_id" +
                " WHERE keyword_id IN" +
                " (SELECT id FROM keyword_100k WHERE keyword = ?)";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, keyword);
        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            ret.add(rs.getString(1));
        }

        return ret;
    }

    public static void main(String[] args) throws SQLException {
        /*
        //Test Q1
        ArrayList<String> test = getAllTitles(2015);
        for (String title : test) {
            System.out.println(title);
        }
        */

        /*
        //Test Q3
        ArrayList<String> test = getAllTitles("ufo");
        for (String title : test) {
            System.out.println(title);
        }
        */
    }
}
