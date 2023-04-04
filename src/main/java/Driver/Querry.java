package Driver;

import org.json.JSONArray;
import org.json.JSONObject;

public interface Querry {



    public JSONObject Insert(String Table_Name, JSONObject values);

    public JSONObject InsertOverwrite(String Table_Name, JSONObject values);



    public JSONObject InsertMulti(String Table_Name, JSONArray values);


    public JSONObject CreateTable(String Table_Name, JSONArray values);


    public JSONObject ModifyColumn(String Table_Name,JSONObject col);

    public JSONObject AddColumnPrimaryKey(String Table_Name,String Constraint_name, JSONObject con);
    public JSONObject AddUniqueKey(String Table_Name,String Constraint_name, JSONObject con);

    public JSONObject AddForeignPrimaryKey(String Table_Name,String Constraint_name, JSONObject con) ;

    public JSONArray Select(String Table_Name, JSONArray col) ;
    public JSONArray Select(String Table_Name, JSONArray col, long Limit) ;

    public JSONArray SelectWith(String Table_Name, JSONArray col, JSONArray condition);

    public JSONArray getTables();
    public JSONArray getTableDesc(String Name);
}
