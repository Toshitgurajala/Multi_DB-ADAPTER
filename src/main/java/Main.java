import Driver.Connector;
import Driver.DatabaseType;
import Driver.MigrateDB;

import java.sql.SQLException;

public class Main {

    public static void main(String[] args) {

        Connector connector2 = new Connector("cse_dept","postgres","Imm@ak@12","localhost",5432,new DatabaseType(1));

        Connector connector = new Connector("hackthon","root","","localhost",3306,new DatabaseType(0));


        try {


            connector.Connect();
            connector2.Connect();
            MigrateDB migrateDB = new MigrateDB(connector,connector2);

            migrateDB.Migrate();




             MySqlExample mySqlExample = new MySqlExample();
//
           mySqlExample.CreateTable();
//
           mySqlExample.Insert();
 //     mySqlExample.InsertOverWrite();

            mySqlExample.Select();

            PSQLExample psqlExample =  new PSQLExample();
            psqlExample.CreateTable();
           psqlExample.Insert();
           psqlExample.Select();

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }



    }


}
