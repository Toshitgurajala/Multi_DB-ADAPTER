package Driver;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MySql implements Querry{

    Connection connection;
    static HashMap<String,String> DataTypes = new HashMap<>();


    public MySql(Connection connection) {

        DataTypes.put("BINT","BIGINT");
        DataTypes.put("TIME","DATE-TIME");
        DataTypes.put ("FLOAT","FLOAT");
        DataTypes.put("DECI","DECIMAL");
        DataTypes.put("NUM","NUMERIC");
        DataTypes.put ("VARC","VARCHAR(100)");
        this.connection = connection;
    }

    @Override
    public JSONObject Insert(String Table_Name, JSONObject values) {
        JSONObject result = new JSONObject();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            String qu = GetMySQLInsert(Table_Name,values);
            System.out.println(qu);
            statement.executeUpdate(qu);
            result.put("Status","Inserted");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            result.put("Status","Erro");
            result.put("Message",e.getMessage());

        }
        finally {
            return result;
        }
    }
    @Override
    public JSONObject InsertMulti(String Table_Name, JSONArray values) {
        JSONObject result = new JSONObject();
        Statement statement = null;
        try {
            statement = connection.createStatement();

            statement.executeUpdate(GetMySQLInsertMulti(Table_Name,values));
            result.put("Status","Inserted");
        } catch (SQLException e) {
            result.put("Status","Erro");
            result.put("Message",e.getMessage());

        }
        finally {
            return result;
        }

    }

    @Override
    public JSONObject InsertOverwrite(String Table_Name, JSONObject values) {
        JSONObject result = new JSONObject();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            String qu = GetMySQLReplace(Table_Name,values);
            System.out.println(qu);
            statement.executeUpdate(qu);
            result.put("Status","Inserted");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            result.put("Status","Erro");
            result.put("Message",e.getMessage());

        }
        finally {
            return result;
        }
    }




    ///ALTER TABLE cse_dept.akshayyy CHANGE Name Name433 varchar(100);
    @Override
    public JSONObject ModifyColumn(String Table_Name, JSONObject col) {
        JSONObject result = new JSONObject();
        StringBuffer xyz = new StringBuffer("ALTER TABLE "+ Table_Name+ " CHANGE ");
        xyz.append(col.getString("oldName") + " ");
        xyz.append(col.getString("newName") + " ");
        xyz.append(DataTypes.get(col.get("type").toString()));
        xyz.append(";");
        Statement statement = null;
        try {
            statement = connection.createStatement();
            System.out.println(xyz);
            statement.executeUpdate(String.valueOf(xyz));
            result.put("Status","Modified");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            result.put("Status","Error");
            result.put("Message",e.getMessage());
            throw new RuntimeException(e);
        }
        finally {
            return result;
        }
    }


    //ALTER TABLE cse_dept.akshayyy ADD CONSTRAINT akshayyy_pk PRIMARY KEY (id);
    //ALTER TABLE Persons ADD CONSTRAINT PK_Person PRIMARY KEY (ID,LastName);
    @Override
    public JSONObject AddColumnPrimaryKey(String Table_Name,String Constraint_name, JSONObject con) {
        JSONObject result = new JSONObject();
        StringBuffer xyz = new StringBuffer("ALTER TABLE "+ Table_Name+ " ADD CONSTRAINT " + Constraint_name + " PRIMARY KEY (");

        List<String> var = new ArrayList<>();
        JSONArray jsonArray = con.getJSONArray("col_names");
        for (int i = 0; i < jsonArray.length(); i++) {

            var.add(jsonArray.get(i).toString());

        }
        xyz.append(String.join(",",var));
        xyz.append(");");

        System.out.println(xyz.toString());
        Statement statement = null;
        try {
            statement = connection.createStatement();
            System.out.println(xyz);
            statement.executeUpdate(String.valueOf(xyz));
            result.put("Status","Added!");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            result.put("Status","Error");
            result.put("Message",e.getMessage());
            throw new RuntimeException(e);
        }
        finally {
            return result;
        }
    }

    @Override
    public JSONObject AddUniqueKey(String Table_Name, String Constraint_name, JSONObject con) {
        return null;
    }

    //ADD FOREIGN KEY (column_name)
    //REFERENCE table_name(Referencing column_name in table_name);

    @Override
    public JSONObject AddForeignPrimaryKey(String Table_Name, String Constraint_name, JSONObject con) {
       JSONObject result = new JSONObject();
        StringBuffer xyz = new StringBuffer("ALTER TABLE "+ Table_Name+ " ADD CONSTRAINT " + Constraint_name + " FOREIGN KEY (" + con.getString("Col_Name") + ") REFERENCES " + con.getString("Rtable") + "(" + con.getString("RCol" )+");");


        System.out.println(xyz.toString());
        Statement statement = null;
        try {
            statement = connection.createStatement();
            System.out.println(xyz);
            statement.executeUpdate(String.valueOf(xyz));
            result.put("Status","Added!");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            result.put("Status","Error");
            result.put("Message",e.getMessage());
            throw new RuntimeException(e);
        }
        finally {
            return result;
        }
    }



    //SELECT ID, Name, Address, Phone_no FROM hackthon.student WHERE ID=0;

    @Override
    public JSONArray Select(String Table_Name, JSONArray col) {
        JSONArray jsonArray = new JSONArray();
        StringBuffer xyz = new StringBuffer("SELECT ");
        if(col.length()>0)
        {
            List<String> set  =new ArrayList<>();
            for(int i =0; i < col.length();i++)
            {
                set.add(col.getString(i));
            }
            xyz.append(String.join(",",set));

        }
        else
        {
            xyz.append(" * ");
        }
        xyz.append(" FROM " + Table_Name + ";");

        System.out.println(xyz.toString());

        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(String.valueOf(xyz));
            while (resultSet.next()) {

                int columns = resultSet.getMetaData().getColumnCount();
                JSONObject obj = new JSONObject();

                for (int i = 0; i < columns; i++)
                    obj.put(resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase(), resultSet.getObject(i + 1));

                jsonArray.put(obj);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        finally {
            return jsonArray;
        }

    }

    @Override
    public JSONArray Select(String Table_Name, JSONArray col, long Limit) {
        JSONArray jsonArray = new JSONArray();
        StringBuffer xyz = new StringBuffer("SELECT ");
        if(col.length()>0)
        {
            List<String> set  =new ArrayList<>();
            for(int i =0; i < col.length();i++)
            {
                set.add(col.getString(i));
            }
            xyz.append(String.join(",",set));

        }
        else
        {
            xyz.append(" * ");
        }
        xyz.append(" FROM " + Table_Name + " LIMIT "+Limit + ";");


        Statement statement = null;
        try {
            statement = connection.createStatement();
            System.out.println(xyz.toString());
            ResultSet resultSet = statement.executeQuery(String.valueOf(xyz));
            while (resultSet.next()) {

                int columns = resultSet.getMetaData().getColumnCount();
                JSONObject obj = new JSONObject();

                for (int i = 0; i < columns; i++)
                    obj.put(resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase(), resultSet.getObject(i + 1));

                jsonArray.put(obj);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        finally {
            return jsonArray;
        }

    }



    @Override
    public JSONArray SelectWith(String Table_Name, JSONArray col, JSONArray condition) {
        JSONArray jsonArray = new JSONArray();
        StringBuffer xyz = new StringBuffer("SELECT ");
        if(col.length()>0)
        {
            List<String> set  =new ArrayList<>();
            for(int i =0; i < col.length();i++)
            {
                set.add(col.getString(i));
            }
            xyz.append(String.join(",",set));

        }
        else
        {
            xyz.append(" * ");
        }
        xyz.append(" FROM " + Table_Name );

        if(condition.length()>0)
        {
            xyz.append(" Where ");
            List<String> set  =new ArrayList<>();
            for(int i =0; i < condition.length();i++)
            {
                JSONObject x = condition.getJSONObject(i);
                set.add(x.getString("Col_Name") + " " + x.getString("Condition"));
            }
            xyz.append(String.join(" and ",set));

        }


        xyz.append(";");

        Statement statement = null;
        try {
            statement = connection.createStatement();
            System.out.println(xyz.toString());
            ResultSet resultSet = statement.executeQuery(String.valueOf(xyz));
            while (resultSet.next()) {

                int columns = resultSet.getMetaData().getColumnCount();
                JSONObject obj = new JSONObject();

                for (int i = 0; i < columns; i++)
                    obj.put(resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase(), resultSet.getObject(i + 1));

                jsonArray.put(obj);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        finally {
            return jsonArray;
        }

    }

    @Override
    public JSONArray getTables() {
        JSONArray jsonArray = new JSONArray();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            System.out.println("Show Tables;");
            ResultSet resultSet = statement.executeQuery("show Tables");
            while (resultSet.next()) {

                jsonArray.put(resultSet.getString(1));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        return jsonArray;
    }

    @Override
    public JSONArray getTableDesc(String Name) {
        JSONObject jsonObject = new JSONObject();
        JSONArray jsonArray = new JSONArray();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            System.out.println("DESC " + Name + ";");
            ResultSet resultSet = statement.executeQuery("DESC " + Name + ";");
            while (resultSet.next()) {

                int columns = resultSet.getMetaData().getColumnCount();
                JSONObject obj = new JSONObject();

                for (int i = 0; i < columns; i++)
                    obj.put(resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase(), resultSet.getObject(i + 1));

                jsonArray.put(obj);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        finally {
            return jsonArray;
        }

    }


    @Override
    public JSONObject CreateTable(String Table_Name, JSONArray values) {
        JSONObject result = new JSONObject();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            String qu = GetMySQLTable(Table_Name,values);
            System.out.println(qu);
            statement.executeUpdate(qu);
            result.put("Status","Created!");

        } catch (Exception e) {
            result.put("Status","Erro");
            result.put("Message",e.getMessage());
            throw new RuntimeException(e);
        }
        finally {
            return result;
        }
    }


    public static String GetMySQLTable(String Name, JSONArray jsonArray )
    {
        StringBuffer xyz = new StringBuffer("CREATE TABLE " + Name + "(");
        List<String> k = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {

            k.add(GenColData(jsonArray.getJSONObject(i)));

        }
        xyz.append(String.join(",",k));
        xyz.append(");");
        System.out.println(xyz);
        return xyz.toString();
    }




    private static String GenColData(JSONObject object)
    {
        StringBuffer buffer = new StringBuffer();
        List<String> var = new ArrayList<>();
        var.add(object.get("field").toString());

        var.add(DataTypes.get(object.get("type").toString()));

        if(object.has("auto_inc") &&(boolean) object.get("auto_inc"))
        {
            var.add("auto_increment");
        }
        if(object.has("nul") && object.getBoolean("null")==false)
        {
            var.add("NOT NULL");
        }

        if(object.has("default"))
        {
            var.add("DEFAULT " + object.get("default"));
        }

        if(object.has("key")) {

            if (object.get("key").toString().toLowerCase().equals("FOREIGN KEY")) {
                JSONObject fkey = (JSONObject) object.get("f_key");
                var.add("FOREIGN KEY REFERENCES " + fkey.get("table") + "(" + fkey.get("col_name") + ")");
            } else {
                var.add(object.getString("key"));
            }
        }

        return String.join(" ",var);
    }

    public static String GetMySQLInsert(String Name, JSONObject jsonObject)
    {
        StringBuffer xyz = new StringBuffer("INSERT INTO " + Name + "(");
        String col = String.join(",",jsonObject.keySet());
        xyz.append(col);
        xyz.append(") VALUES(");
        List<String> val = new ArrayList<>();
        for (String key:jsonObject.keySet()
        ) {
            Object o = jsonObject.get(key);
            if(getDataType(o))
            {
                val.add("'"+o+"'");
            }
            else
            {
                val.add(o.toString());
            }
        }

        xyz.append(String.join(",",val));
        xyz.append(");");
        return xyz.toString();

    }

    public static String GetMySQLReplace(String Name, JSONObject jsonObject)
    {
        StringBuffer xyz = new StringBuffer("REPLACE INTO " + Name + "(");
        String col = String.join(",",jsonObject.keySet());
        xyz.append(col);
        xyz.append(") VALUES(");
        List<String> val = new ArrayList<>();
        for (String key:jsonObject.keySet()
        ) {
            Object o = jsonObject.get(key);
            if(getDataType(o))
            {
                val.add("'"+o+"'");
            }
            else
            {
                val.add(o.toString());
            }
        }

        xyz.append(String.join(",",val));
        xyz.append(");");
        return xyz.toString();

    }

    public static String GetMySQLInsertMulti(String Name, JSONArray jsonArray)
    {
        JSONObject jsonObject = jsonArray.getJSONObject(0);
        StringBuffer xyz = new StringBuffer("INSERT INTO " + Name + "(");
        String col = String.join(",",jsonObject.keySet());
        xyz.append(col);
        xyz.append(") VALUES ");

        List<String> multi = new ArrayList<>();
        for(int i=0;i<jsonArray.length();i++)
        {
            jsonObject = jsonArray.getJSONObject(i);
            List<String> val = new ArrayList<>();
            for (String key:jsonObject.keySet()
            ) {
                Object o = jsonObject.get(key);
                if(getDataType(o))
                {
                    val.add("'"+o+"'");
                }
                else
                {
                    val.add(o.toString());
                }
            }
            multi.add("("+String.join(",",val)+")");
        }

        xyz.append(String.join(",",multi));
        xyz.append(";");
        return xyz.toString();

    }

    private static boolean getDataType(Object object)
    {
        if(object instanceof String || object instanceof Character)
        {
            return  true;
        }
        return false;
    }



}
