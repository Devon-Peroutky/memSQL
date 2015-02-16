import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Singleton to get Connection for making database queries.
 * 
 * Remember to close the connection once you're done!
 * @return
 * Returns Connection. null if unsuccessful.
 */
public class DBConn {
	private static DBConn _instance = new DBConn();
	private Connection conn;

	protected DBConn() {
		this.conn = createConnection();
	}
	
	public static DBConn getInstance() {
		if (DBConn._instance == null) {
			DBConn._instance = new DBConn();
		}
		return DBConn._instance;
	}
	
	public Connection getConnection() {
		return conn;
	}
	
	public Connection createConnection() {
		Connection conn = null;
		String userName = "root";
		String password = "";
		String url = "jdbc:mysql://127.0.0.1:3306/";
		String db = "MemEx";

		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection(url+db+"?user="+userName);
			return conn;
		} catch (Exception e) {
			System.err.println("Connection failed. Did you create the loca MySQL Database?");
			e.printStackTrace();
			try {
				conn.close();
			} catch (SQLException ignore) {

			}
			return null;

		}
	}
}