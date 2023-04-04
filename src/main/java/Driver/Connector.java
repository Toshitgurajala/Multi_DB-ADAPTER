package Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class Connector {
    public  String DATABASE_NAME ;
    public  String DATABASE_USERNAME;
    public  String DATABASE_PASSWORD;
    public  String HOST;
    public int PORT;

    public  DatabaseType TYPE ;


    Connection MYSQLconnection;
    Connection PSQLconnection;



    public Connector(String DATABASE_NAME, String DATABASE_USERNAME, String DATABASE_PASSWORD, String HOST, int PORT, DatabaseType TYPE) {
        this.DATABASE_NAME = DATABASE_NAME;
        this.DATABASE_USERNAME = DATABASE_USERNAME;
        this.DATABASE_PASSWORD = DATABASE_PASSWORD;
        this.HOST = HOST;
        this.PORT = PORT;
        this.TYPE = TYPE;
    }



    public boolean Connect() throws ClassNotFoundException, SQLException {
        if(TYPE.getType()==0)
        {
            Class.forName("com.mysql.cj.jdbc.Driver");
            MYSQLconnection =  DriverManager.getConnection("jdbc:mysql://"+HOST+":"+PORT + "/"+DATABASE_NAME,
                    DATABASE_USERNAME, DATABASE_PASSWORD);
            System.out.println(MYSQLconnection.getMetaData());
            System.out.println("MYSQL CONNECTED");
        }
        if(TYPE.getType()==1)
        {
            Class.forName("org.postgresql.Driver");
            PSQLconnection =  DriverManager.getConnection("jdbc:postgresql://"+HOST+":"+PORT + "/"+DATABASE_NAME,
                    DATABASE_USERNAME, DATABASE_PASSWORD);
            System.out.println(PSQLconnection.getMetaData());
            System.out.println("POSTgres");
        }

        return false;
    }


    public Querry getQuerry()
    {
        if(TYPE.getType() ==0)
        {
            return new MySql(MYSQLconnection);
        }
        if(TYPE.getType()==1)
        {
            return new PostgresSQl(PSQLconnection);
        }
        return null;
    }




}
