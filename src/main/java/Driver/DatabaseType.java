package Driver;

import java.util.HashMap;

public class DatabaseType {

    int type;
    HashMap<Integer,String> Database  = new HashMap<>();


    public DatabaseType(int type) {
        Database.put(0,"MYSQL");
        Database.put(1,"PostgresSQl");
        Database.put(2,"Hive");

        this.type = type;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getDatabase() {
        return Database.get(type);
    }


}
