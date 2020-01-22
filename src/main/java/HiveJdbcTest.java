import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class HiveJdbcTest {
  public static void main(String[] args) throws ClassNotFoundException, SQLException {

    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;


    if (args.length != 3) {
      System.err.println("Usage: HiveJdbcTest <jdbc-uri> <user> <password>");
      System.exit(-1);
    }


    Class.forName("org.apache.hive.jdbc.HiveDriver");

     connection = DriverManager.getConnection(args[0], args[1], args[2]);


        try{

          connection=DriverManager.getConnection
             (args[0], args[1], args[2]);

             // first arg is : DB_URL jdbc:hive2://localhost:3306/databasebname
             // second arg is: DB_USER 
             // third arg is : DB_PASSWD

             // java -jar  app.jar arg1 arg2 arg3
          
          
          
          statement=connection.createStatement();
          
          resultSet=statement.executeQuery("SELECT * FROM books");
          
          while(resultSet.next()){
              
             System.out.printf("%s\t%s\t%s\t%f\n",
                     
             resultSet.getString(1),
             resultSet.getString(2),
             resultSet.getString(3),
             resultSet.getFloat(4));
          }
 
          
       }catch(SQLException ex){
       }finally{
          try {
             resultSet.close();
             statement.close();
             connection.close();
          } catch (SQLException ex) {
          }
       }


  }
}
