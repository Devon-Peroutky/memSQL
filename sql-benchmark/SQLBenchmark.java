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
    public static Connection conn = null;
    private static int iterations = 15;
    private static String localValuesPath = "/home/ubuntu/workspace/Interviews/src/memSQL/sql-benchmark/values.txt";
    private static String clusterValuesPath = "/home/ubuntu/code/src/memSQL/sql-benchmark/values.txt";

    private static void printResult(String name, long ms, long bytes, int i) {
        System.out.println(
                name + ": (" + iterations + ")\n\t\t" +
                (ms / (1000.0*i)) + " seconds per Full-Table Operation \n\t\t" +
                (bytes / 1000.0) + " MBs used \n\t\t" +
                Math.round(1000.0 * i / ms) + " transactions/second \n\t\t" 
        );
    }

    // This is a Bulk insert using LOAD DATA INFILE, to see a 'manuel' INSERT, look in dataGenerator.py
    public static int insert() throws Exception {
        // Declarations
        int rowCount=0;
        Statement stmt = null;
        stmt = conn.createStatement();
        String loadFile = "LOAD DATA LOCAL INFILE \'" +clusterValuesPath+"\' INTO TABLE SCANS FIELDS TERMINATED BY \',\' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\n\';";
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

    private static int selectDistinct(String parameter) throws Exception {
        // Declarations
        String sql = "SELECT COUNT(DISTINCT "+parameter+") FROM SCANS";

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

    public static int select() throws Exception {
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
        // Declarations
    	int transactions=0;
    	totalTime=0;
	
        Statement stmt = null;
        stmt = conn.createStatement();

        // DELETE, INSERT, SELECT
        for(int i=0; i<iterations; i++) {
            // Create threads that will run simultaneously
            SQLThread t1 = new SQLThread("insert");
            SQLThread t2 = new SQLThread("select");

            // DELETE all entries
	        stmt.executeUpdate("DELETE FROM SCANS WHERE SCAN_COUNT<8");

	        // Initialize Timer
        	startTotalMem = runtime.totalMemory()-runtime.freeMemory();
	        startTime = System.currentTimeMillis();

    		// Start the INSERT and QUERY thread
    		t1.start();
            t2.start();

	        // Stop Timer
        	endTime = System.currentTimeMillis();
            totalTime+=endTime-startTime;	
            transactions+=t1.transactions;
            transactions+=t2.transactions;
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

    private static void close(Statement stmt, ResultSet rs, Connection conn) {
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

    /**
     * @param args the command line arguments
     */
	public static void main(String[] args) {

        // Connect to database
        conn = DBConn.getInstance().getConnection();

        // Run the BenchMarks here
        try {
            stmt = conn.createStatement();
            stmt.setPoolable(true);

            // Benchmarks
            //insert();
    	    selectDistinct("SCAN_HASH");
            selectDistinct("SCAN_ID");
    	    select();
    	    selectWhere("SCAN_HASH=1000000");
    	    selectWhere("SCAN_ID=1000000");
            selectWhere("SCAN_HASH<25000000");
            selectWhere("SCAN_ID<25000000");
            selectWhere("SCAN_HASH>40000000");
            selectWhere("SCAN_ID>40000000");
    	    mixed();
        } catch (Exception e) {
            System.out.println("Error w/Benchmarks");
            System.err.println(e);
        } finally {
            close(stmt, rs, conn);
        }
    }   
}