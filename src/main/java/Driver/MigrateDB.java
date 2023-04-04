package Driver;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class MigrateDB {
    Connector c1;
    Connector c2;

    Querry q1;
    Querry q2;

    public MigrateDB(Connector c1, Connector c2) {
        this.c1 = c1;
        this.c2 = c2;
    }

    public void Migrate() {
        q1 = c1.getQuerry();
        q2 = c2.getQuerry();

        // GEt All Tables from C1

        JSONArray res = q1.getTables();

        List<JSONArray> descs = new ArrayList<>();
        // Create Tables
        //loop over tables
        for (int i = 0; i < res.length(); i++) {
            System.out.println("Migrating Table " + res.getString(i));

            JSONArray fields = q1.getTableDesc(res.getString(i));
            //Loop Over fields

            JSONArray table = new JSONArray();
            JSONArray Primary_keys = new JSONArray();
            for (int j = 0; j < fields.length(); j++) {
                String k = MigratePrimaryKeys(fields.getJSONObject(j));
                if(k.length()>0)
                {
                    Primary_keys.put(k);
                }
                table.put(MigrateFields(fields.getJSONObject(j)));
            }

            c2.getQuerry().CreateTable(res.getString(i),table);
            JSONObject j = new JSONObject();
            j.put("col_names",Primary_keys);
            if(j.length()>0)
                c2.getQuerry().AddColumnPrimaryKey(res.getString(i),res.getString(i) +"_PK",j);
            table.clear();
            Primary_keys.clear();
            j.clear();

            JSONArray res1 = q1.Select(res.getString(i), new JSONArray());
            System.out.println("Inserting Values");
            System.out.println(q2.InsertMulti(res.getString(i),res1).toString());
            System.out.println("Table : " + res.getString(i) + " Migrated ");
        }



    }

    private static String MigratePrimaryKeys(JSONObject jsonObject) {

        if(jsonObject.has("key") && jsonObject.getString("key").contains("PRI"))
        {
            return  jsonObject.getString("field");
        }

        return "";
    }

    private static JSONObject MigrateFields(JSONObject object)
    {
        JSONObject mapped = object;

        if(mapped.getString("type").contains("int") )
        {
            if( mapped.getString("extra").equals("auto_increment"))
            {
                mapped.put("type","auto");

            }else
            {
                mapped.put("type","bint");

            }
        }
        else if(mapped.getString("type").contains("") || mapped.getString("type").contains("text"))
        {
            mapped.put("type","varc");

        }

        mapped.remove("key");
//        if(mapped.getString("key").contains("PRI"))
//        {
//            mapped.put("key","Primary Key");
//        }
//        else if(mapped.getString("key").contains("UNI"))
//        {
//            mapped.put("key","UNIQUE");
//        }
//        else if(mapped.getString("key").contains("MUL"))
//        {
//            mapped.remove("key");
//        }
        String getNull = mapped.getString("null");
        if(getNull.equals("YES"))
        {
            mapped.put("null",true);
        }
        else
        {
            mapped.put("null",false);

        }
        return mapped;
    }


}
