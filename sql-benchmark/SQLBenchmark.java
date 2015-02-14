import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLBenchmark {

    private static Statement stmt  = null;
    private static ResultSet rs    = null;
    private static Runtime runtime = Runtime.getRuntime();
    private static long startTime, endTime;
    private static long startTotalMem;
    private static PreparedStatement pStmt;
    private static int num;
    private static Connection conn = null;


    private static void printResult(String name, long ms, long bytes, int i) {
        System.out.println(
            name + ": " +
            (ms / 1000.0) + "s   \t " +
            Math.round(1000.0 * i / ms) + " queries/second   \t"
        );
    }

    private static void simpleExec() throws Exception {
        // SimpleExec
        startTotalMem = runtime.totalMemory()-runtime.freeMemory();
        startTime = System.currentTimeMillis();
        for(int i = 0; i < 500000; i++) {
            stmt.execute("DO 1");
        }
        endTime = System.currentTimeMillis();
        SQLBenchmark.printResult(
            "SimpleExec",
            (endTime-startTime),
            (runtime.totalMemory()-runtime.freeMemory()-startTotalMem),
            500000
        );
    }

    private static void preparedExec() throws Exception {
        // PreparedExec
        startTotalMem = runtime.totalMemory()-runtime.freeMemory();
        startTime = System.currentTimeMillis();
        pStmt = conn.prepareStatement("DO 1");
        for(int i = 0; i < 500000; i++) {
            pStmt.execute();
        }
        pStmt.close();
        endTime = System.currentTimeMillis();
        SQLBenchmark.printResult(
            "PreparedExec",
            (endTime-startTime),
            (runtime.totalMemory()-runtime.freeMemory()-startTotalMem),
            500000
        );
    }

    private static void simpleRowQuery() throws Exception {
        // SimpleQueryRow
        startTotalMem = runtime.totalMemory()-runtime.freeMemory();
        startTime = System.currentTimeMillis();
        for(int i = 0; i < 500000; i++) {
            if (stmt.execute("SELECT 1")) {
                rs = stmt.getResultSet();

                if(rs.next()) {
                    num = rs.getInt(1);
                } else {
                    throw new Exception("No result");
                }
            } else {
                throw new Exception("No result");
            }
        }
        endTime = System.currentTimeMillis();
        SQLBenchmark.printResult(
            "SimpleQueryRow",
            (endTime-startTime),
            (runtime.totalMemory()-runtime.freeMemory()-startTotalMem),
            500000
        );
    }

    private static void preparedQueryRow() throws Exception {
        // PreparedQueryRow
        startTotalMem = runtime.totalMemory()-runtime.freeMemory();
        startTime = System.currentTimeMillis();
        pStmt = conn.prepareStatement("SELECT 1");
        for(int i = 0; i < 500000; i++) {
            if (pStmt.execute()) {
                rs = pStmt.getResultSet();

                if(rs.next()) {
                    num = rs.getInt(1);
                } else {
                    throw new Exception("No result");
                }
            } else {
                throw new Exception("No result");
            }
        }
        pStmt.close();
        endTime = System.currentTimeMillis();
        SQLBenchmark.printResult(
            "PreparedQueryRow",
            (endTime-startTime),
            (runtime.totalMemory()-runtime.freeMemory()-startTotalMem),
            500000
        );
    }

    private static void preparedQueryRowParam() throws Exception {
        // PreparedQueryRowParam
        startTotalMem = runtime.totalMemory()-runtime.freeMemory();
        startTime = System.currentTimeMillis();
        pStmt = conn.prepareStatement("SELECT ?");
        for(int i = 0; i < 500000; i++) {
            pStmt.setInt(1, i);
            if (pStmt.execute()) {
                rs = pStmt.getResultSet();

                if(rs.next()) {
                    num = rs.getInt(1);
                } else {
                    throw new Exception("No result");
                }
            } else {
                throw new Exception("No result");
            }
        }
        pStmt.close();
        endTime = System.currentTimeMillis();
        SQLBenchmark.printResult(
            "PreparedQueryRowParam",
            (endTime-startTime),
            (runtime.totalMemory()-runtime.freeMemory()-startTotalMem),
            500000
        );        
    }

    // executeUpdate() for INSERT
    // executeUpdate() for SELECT

    public static void insert(ArrayList<String> queries) throws Exception {
        Statement stmt = conn.createStatement();
        for (String query : queries) {
            System.out.println(query);
            stmt.executeUpdate(query);
        }
    }

    public static void selects() throws Exception {
        String sql = "SELECT * FROM SCANS";
        ResultSet rs = stmt.executeQuery(sql);

        System.out.println("Inserts");
    }

    public static void mixed() throws Exception {
        System.out.println("Inserts");
    }

    public static ArrayList<String> readQueries(String filename) throws Exception {
        ArrayList<String> result = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = br.readLine()) != null) {
            // process the line.
            result.add(line);
        }
        br.close();
        return result;
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // Declarations
        ArrayList<String> queries = null;

        // Initialize JDBC connector
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            System.err.println("Loading MySQL-Driver failed!");
        }

        // Read Queries
        try {
            queries = readQueries("queries.txt");
        } catch (Exception e) {
            System.out.println("Error reading in Queries from ./queries.txt");
            e.printStackTrace();
        }

        // Connect to database
        try {
            conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/memSQL?user=root&password=");
        } catch (SQLException ex) {
            if(conn != null) {
                try {
                    conn.close();
                } catch(Exception iDontCare) {
                } finally {
                    conn = null;
                }
            }
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.exit(1);
        }

        // Run the BenchMarks here
        try {
            stmt = conn.createStatement();
            stmt.setPoolable(true);

            // Benchmarks
            insert(queries);
            /*
            simpleExec();
            preparedExec();
            simpleRowQuery();
            preparedQueryRow();
            preparedQueryRowParam();
            */

        } catch (Exception e) {
            System.err.println(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException iDontCare) {
                    System.out.println("Error closing ResultSet");
                } finally {
                    rs = null;
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException iDontCare) {
                    System.out.println("Error closing Statement");
                } finally {
                    stmt = null;
                }
            }

            try {
                conn.close();
            } catch(Exception iDontCare) {
                System.out.println("Error closing Connection");
            } finally {
                conn = null;
            }
        }
    }
}
