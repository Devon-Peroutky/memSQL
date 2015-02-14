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
            Math.round(1000.0 * i / ms) + " queries/second   \t "
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

    public static void insert(ArrayList<String> queries) throws Exception {
        // Declarations
        Statement stmt = null;
        stmt = conn.createStatement();
        stmt.executeUpdate("DELETE FROM SCANS WHERE SCAN_COUNT<8");

        // Initialize Timer
        startTotalMem = runtime.totalMemory()-runtime.freeMemory();
        startTime = System.currentTimeMillis();

        // Execute
        for (String query : queries) {
            stmt.executeUpdate(query);
        }
    
        // Stop Timer
        stmt.close();
        endTime = System.currentTimeMillis();

        // Display Output
        SQLBenchmark.printResult(
            "Insert",
            (endTime-startTime),
            (runtime.totalMemory()-runtime.freeMemory()-startTotalMem),
            queries.size()
        );  

    }

    public static void selects(int iterations) throws Exception {
        // Declarations
        String sql = "SELECT * FROM SCANS";

        // Initialize Timer
        startTotalMem = runtime.totalMemory()-runtime.freeMemory();
        startTime = System.currentTimeMillis();

        // Execute
        for (int i=0; i<iterations; i++) {
            ResultSet rs = stmt.executeQuery(sql);
        }

        // Stop Timer
        endTime = System.currentTimeMillis();

        // Display Results
        SQLBenchmark.printResult(
            "SELECT * FROM SCANS",
            (endTime-startTime),
            (runtime.totalMemory()-runtime.freeMemory()-startTotalMem),
            iterations
        );  
    }

    public static void mixed(ArrayList<String> queries) throws Exception {
        // Declarations
        Statement stmt = null;
        String select = "SELECT * FROM SCANS";
        int n1 = queries.size()/4, n2 = queries.size()*2/4, n3 = queries.size()*3/4, n4 = queries.size();
        stmt = conn.createStatement();
        stmt.executeUpdate("DELETE FROM SCANS WHERE SCAN_COUNT<8");


        // Initialize Timer
        startTotalMem = runtime.totalMemory()-runtime.freeMemory();
        startTime = System.currentTimeMillis();

        /*
            I know this section is ugly, but I didn't want any 
            logic in the timed region to potentially distort the
            Benchmark results
        */
        for (int i=0; i<n1; i++) stmt.executeUpdate(queries.get(i));
        stmt.executeQuery(select);
        for (int i=n1; i<n2; i++) stmt.executeUpdate(queries.get(i));
        stmt.executeQuery(select);
        for (int i=n2; i<n3; i++) stmt.executeUpdate(queries.get(i));
        stmt.executeQuery(select);
        for (int i=n3; i<n4; i++) stmt.executeUpdate(queries.get(i));
        stmt.executeQuery(select);

        // Stop Timer
        endTime = System.currentTimeMillis();

        // Display Results
        // n4 = n inserts
        // n4+n3+n2+n1 queries
        SQLBenchmark.printResult(
            "Mixed (40% Inserts, 60% SELECT * FROM SCANS)",
            (endTime-startTime),
            (runtime.totalMemory()-runtime.freeMemory()-startTotalMem),
            n4+n4+n3+n2+n1
        );  

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
            selects(15);
            mixed(queries);

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
