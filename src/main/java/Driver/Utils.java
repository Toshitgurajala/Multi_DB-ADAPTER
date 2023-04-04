package Driver;

import java.util.HashMap;

public class Utils
{
  public   HashMap<String,String> DataTypes = new HashMap<>();

    public Utils()
    {
       DataTypes.put("BINT","BIGINT");
        DataTypes.put   ("TIME","DATE-TIME");
        DataTypes.put ("FLOAT","FLOAT");
        DataTypes.put("DECI","DECIMAL");
        DataTypes.put("NUM","NUMERIC");
        DataTypes.put ("VARC","VARCHAR");
    }

    public HashMap<String, String> getDataTypes() {
        return DataTypes;
    }

    public void setDataTypes(HashMap<String, String> dataTypes) {
        DataTypes = dataTypes;
    }
}
