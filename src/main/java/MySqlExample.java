import Driver.Connector;
import Driver.DatabaseType;
import Driver.Querry;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.SQLException;

public class MySqlExample {
    Connector connector;
    public MySqlExample() throws SQLException, ClassNotFoundException {
         connector = new Connector("ExampleDB","root","","localhost",3306,new DatabaseType(0));
        connector.Connect();
    }



    public void CreateTable()
    {
        JSONArray jsonArray = new JSONArray();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("field","id");
        jsonObject.put("type","BINT");
        jsonObject.put("auto_inc",true);
        jsonObject.put("key","PRIMARY KEY");

        JSONObject jsonObject2 = new JSONObject();
        jsonObject2.put("field","Name");
        jsonObject2.put("type","VARC");
        jsonObject2.put("null",false);

        JSONObject jsonObject3 = new JSONObject();
        jsonObject3.put("field","City");
        jsonObject3.put("type","VARC");
        jsonObject3.put("null",false);
        jsonArray.put(jsonObject);
        jsonArray.put(jsonObject2);
        jsonArray.put(jsonObject3);


        Querry querry = connector.getQuerry();
        JSONObject reposne =  querry.CreateTable("ExampleTable",jsonArray);
        System.out.println(reposne.toString());

    }

    public void Insert()
    {

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("Name","Akshay");
        jsonObject.put("City","Examba");

        Querry querry = connector.getQuerry();
        JSONObject reposne =  querry.Insert("ExampleTable",jsonObject);
        System.out.println(reposne.toString());


    }


    public void InsertOverWrite()
    {

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id",4);
        jsonObject.put("Name","Aniket");
        jsonObject.put("City","Examba");

        Querry querry = connector.getQuerry();
        JSONObject reposne =  querry.InsertOverwrite("ExampleTable",jsonObject);
        System.out.println(reposne.toString());


    }
    public void Select()
    {

        JSONArray jsonArray = new JSONArray();
        // Select ALL
        Querry querry = connector.getQuerry();
        JSONArray reposne =  querry.Select("ExampleTable",jsonArray);
        System.out.println(reposne.toString());

        //Select Col
        jsonArray.put("Name");
        reposne =  querry.Select("ExampleTable",jsonArray);
        System.out.println(reposne.toString());

        //Select With Condition
        JSONArray condi = new JSONArray();
        JSONObject condi1 = new JSONObject();
        condi1.put("Col_Name","ID");
        condi1.put("Condition",">2");

        JSONObject condi2 = new JSONObject();
        condi2.put("Col_Name","ID");
        condi2.put("Condition","<99");
        condi.put(condi1);

        condi.put(condi2);

        reposne =  querry.SelectWith("ExampleTable",new JSONArray(),condi);
        System.out.println(reposne.toString());

    }
}
