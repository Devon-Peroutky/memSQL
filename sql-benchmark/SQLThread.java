import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class SQLThread implements Runnable {
  private Thread t;
  private String query;
  private SQLBenchmark s;
  public int transactions;

  public SQLThread( String type ){

      s = new SQLBenchmark();
      query = type.toLowerCase();
  }

  public void run(){
    if (query.equals("insert"))
      try {
        transactions = s.insert();
      } catch (Exception e) {
        System.out.println("Error");
      }
    else
      try {
        transactions = s.selects();
      } catch (Exception e) {
        System.out.println("Error");
      }
     
    System.out.println("Thread " +  query + " exiting.");
  }

  public void start () {
    System.out.println("Starting " +  query );
    if (t == null) {
       t = new Thread (this, query);
       t.start ();
    }
  }
}