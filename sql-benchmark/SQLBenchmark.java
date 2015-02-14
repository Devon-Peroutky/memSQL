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
    private static long startTime, endTime, totalTime;
    private static long startTotalMem;
    private static PreparedStatement pStmt;
    private static int num;
    private static Connection conn = null;
    private static int iterations = 5;
    private static int total = 6801076;

    private static void printResult(String name, long ms, long bytes, int i) {
        System.out.println(
                name + ": (" + iterations + ")\n\t\t" +
                (ms / (1000.0*i)) + " seconds per Full-Table Scan \n\t\t" +
                (bytes / 1000.0) + " MBs used \n\t\t" +
                Math.round(1000.0 * i / ms) + " transactions/second \n\t\t" 
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

    private static int insert() throws Exception {
        System.out.println("Benchmarking INSERT throughput");

        // Declarations
	int rowCount=0;
        Statement stmt = null;
	stmt = conn.createStatement();
	String loadFile = "LOAD DATA INFILE \'/home/ubuntu/code/memSQL/sql-benchmark/values.txt\' INTO TABLE SCANS FIELDS TERMINATED BY \',\' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\n\' IGNORE 1 LINES;";

        stmt.executeUpdate("DELETE FROM SCANS WHERE SCAN_COUNT<8");

        // Initialize Timer
        startTotalMem = runtime.totalMemory()-runtime.freeMemory();
        startTime = System.currentTimeMillis();

        // Execute
	stmt.executeUpdate(loadFile);
    
        // Stop Timer
        stmt.close();
        endTime = System.currentTimeMillis();

	// Get number of records written
        stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM SCANS");
	rs.next();
	rowCount = rs.getInt(1);

        // Display Output
        SQLBenchmark.printResult(
            "Insert",
            (endTime-startTime),
            (runtime.totalMemory()-runtime.freeMemory()-startTotalMem),
           rowCount 
        ); 	 
	return rowCount;
    }

    private static void selectWhere(String where) throws Exception{
        System.out.println("Benchmarking SELECT WHERE throughput");

        // Declarations
        String sql = "SELECT * FROM SCANS WHERE " + where;

        // Initialize Timer
        startTotalMem = runtime.totalMemory()-runtime.freeMemory();
        startTime = System.currentTimeMillis();

        // Execute
        for (int i=0; i<iterations; i++) 
            stmt.executeQuery(sql);

        // Stop Timer
        endTime = System.currentTimeMillis();

        // Display Results
        SQLBenchmark.printResult(
            sql,
            (endTime-startTime),
            (runtime.totalMemory()-runtime.freeMemory()-startTotalMem),
            iterations
        ); 
    }

    private static int selects() throws Exception {
        System.out.println("Benchmarking SELECT throughput");

        // Declarations
        String sql = "SELECT * FROM SCANS";

        // Initialize Timer
        startTotalMem = runtime.totalMemory()-runtime.freeMemory();
        startTime = System.currentTimeMillis();

        // Execute
        for (int i=0; i<iterations; i++) 
            stmt.executeQuery(sql);

        // Stop Timer
        endTime = System.currentTimeMillis();

        // Display Results
        SQLBenchmark.printResult(
            sql,
            (endTime-startTime),
            (runtime.totalMemory()-runtime.freeMemory()-startTotalMem),
	    iterations            
        ); 
	return iterations; 
    }

    private static void mixed() throws Exception {
	System.out.println("Benchmarking MIXED throughput");

        // Declarations
	int transactions=0;
	totalTime=0;
	
        Statement stmt = null;
        stmt = conn.createStatement();

	// DELETE, INSERT, SELECT
	for(int i=0; i<iterations; i++) {
		// DELETE all entries
	        stmt.executeUpdate("DELETE FROM SCANS WHERE SCAN_COUNT<8");

	        // Initialize Timer
        	startTotalMem = runtime.totalMemory()-runtime.freeMemory();
	        startTime = System.currentTimeMillis();

		// INSERT & Query		
		transactions+=insert();
		transactions+=selects();

	        // Stop Timer
        	endTime = System.currentTimeMillis();
		totalTime+=endTime-startTime;	
	}

        // Display Results
        // n4 = n inserts
        // n4+n3+n2+n1 queries
        SQLBenchmark.printResult(
            "Mixed",
            totalTime,
            (runtime.totalMemory()-runtime.freeMemory()-startTotalMem),
            transactions
        );  
    }

    private static ArrayList<String> readQueries(String filename) throws Exception {
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
        // Initialize JDBC connector
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            System.err.println("Loading MySQL-Driver failed!");
        }

        // Connect to database
        try {
            conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/MemEx?user=root&password=");
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
	    insert();
	    selects();
	    selectWhere("SCAN_ID=10000000");
	    mixed();
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
