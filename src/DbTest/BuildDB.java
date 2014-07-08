package DbTest;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import nl.knaw.dans.common.dbflib.CorruptedTableException;
import nl.knaw.dans.common.dbflib.IfNonExistent;
import nl.knaw.dans.common.dbflib.Record;
import nl.knaw.dans.common.dbflib.Table;

public class BuildDB {
    private static final String hist = "base_historic.db3";
    private static final String conv = "base_conventional.db3";
    private static final String inDB = "spatial.db3";
    private static final String[] tbl_names = {"crop_economic_farms", "crop_economic_fields", "crop_economic_subbasins", "forage", "forage_hru", "tillage",
                                               "grazing_hrus", "grazing_economic", "grazing_economic_subbasins", "small_dams_economic", "holding_ponds_economic",
                                               "grazing", "holding_ponds", "small_dams", "tillage_hrus"};
    private static final String[] val_names = {"id", "year", "yield", "revenue", "cost", "net_return", "grazing_ha", "unit_cost", "hru", "cattle",
                                               "clay_liner", "plastic_ln", "wire_fence", "distance", "trenching", "pond_yrs", "annual_cost",
                                               "maintenance", "total_cost", "embankment", "life_time", "tillage"};
    private static final String[] types = {"int", "real"};
    private static final File[] dbf_tbls = {new File("Data/Spatial/small_dam.dbf"), new File("Data/Spatial/cattle_yard.dbf"), new File("Data/Spatial/grazing.dbf")};
    // "land2010_by_land_id", "farm2010"
    
    /**
     * 
     * @param in: Input Statement Call. This contains the Connection to the Input SQLite3 DB file.
     * @param out: Output Statement Call. This contains the Connection to the Output SQLite3 DB file.  
     */
    
    private static void buildTables(Statement in, Statement out) {
        try {
            int i = 1, t = 1;
            for (String tbl: tbl_names) {
                String sql = "CREATE TABLE IF NOT EXISTS " + tbl + "(";
                switch(i) {
                    case 1:
                        sql += val_names[0] + " " + types[0] + ", " + val_names[1] + " " + types[0] + ", " + val_names[2] + " " + types[1] + ", "
                             + val_names[3] + " " + types[1] + ", " + val_names[4] + " " + types[1] + ", " + val_names[5] + " " + types[1] + ");";
                        t++;
                        if(t > 3) {
                            i++;
                            t = 1;
                        }
                        break;
                    case 2:
                        sql += val_names[0] + " " + types[0] + ");";
                        t++;
                        if(t > 4) {
                            i++;
                            t = 1;
                        }
                        break;
                    case 3:
                        sql += val_names[0] + " " + types[0] + ", " + val_names[1] + " " + types[0] + ", " + val_names[4] + " " + types[1] + ");";
                        t++;
                        if(t > 4) {
                            i++;
                            t = 1;
                        }
                        break;
                    case 4:
                        sql += val_names[0] + " " + types[0] + ", " + val_names[6] + " " + types[1] + ", " + val_names[7] + " " + types[1] + ", " + val_names[4] + " " + types[1] + ");";
                        i++;
                        break;
                    case 5:
                        sql += val_names[0] + " " + types[0] + ", " + val_names[8] + " " + types[0] + ", " + val_names[9] + " " + types[1] + ", "
                            + val_names[10] + " " + types[1] + ", " + val_names[11] + " " + types[1] + ", " + val_names[12] + " " + types[1] + ", "
                            + val_names[13] + " " + types[1] + ", " + val_names[14] + " " + types[1] + ", " + val_names[15] + " " + types[1] + ", "
                             + val_names[4] + " " + types[1] + ", " + val_names[16] + " " + types[1] + ", " + val_names[17] + " " + types[1] + ", "
                            + val_names[18] + " " + types[1] + ");";
                        i++;
                        break;
                    case 6:
                        sql += val_names[0] + " " + types[0] + ", " + val_names[19] + " " + types[1] + ", " + val_names[20] + " " + types[0] + ");";
                        i++;
                        break;
                    case 7:
                        sql += val_names[0] + " " + types[0] + ", " + val_names[21] + " " + types[0] + ");";
                        i++;
                        break;
                    default:
                        sql = "EoE";
                        i = 0;
                        break; 
                }
                if(sql.equalsIgnoreCase("EoE")) {
                    System.out.println("\nTABLES CREATED");
                }
                else {
                    out.executeUpdate(sql);
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(BuildDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * 
     * @param in: Input Statement Call. This contains the Connection to the Input SQLite3 DB file.
     *            For all subsequent fillTableX functions, in refers to the Input connection. Any
     *            variables having the prefix 'in' are used in relation to values taken from the input tables
     *            via the input DB connection. 
     * @param out: Output Statement Call. This contains the Connection to the Output SQLite3 DB file.
     *             For all subsequent fillTableX functions, in refers to the Output connection. Any
     *             variables having the prefix 'out' are used in relation to values taken from/written to the output table
     *             via the output DB connection. 
     * @param tbl: Name of the Input Table called yield_historic from the SQLite3 DB.
     */
    
    private static void fillCropEconFields(Statement in, Statement out, String tbl) {
        try {
            ResultSet inRs = in.executeQuery("SELECT * FROM " + tbl + ";");
            ResultSet outRs = out.executeQuery("SELECT * FROM " + tbl_names[1] + ";");
            NameTypePair[] ntp = getInputNamesAndTypes(inRs);
            String outColumnNames = loadOutputSqlColumnNames(outRs);
            String sql = "INSERT INTO " + tbl_names[1] + "(" + outColumnNames + "VALUES(";
            while(inRs.next()) {
                out.executeUpdate(buildFieldOutputQuery(inRs, ntp, sql, 0));
            }
        } catch (SQLException e) {
            Logger.getLogger(BuildDB.class.getName()).log(Level.SEVERE, null, e); 
        }
    }
    
    /**
     * 
     * @param inFld: Input Statement for the yield_historic SQL Table. Referred
     *               to as Fld since it is used to build crop_Economic_Fields
     *               table. Additionally, each farm/subbasin is comprised of the
     *               fields from this table.
     * @param inBmp: Input Statement for the field_farm SQL Table.
     *               Referred to as inBMP to avoid duplication of the functions
     *               used to build the output SQL Tables. Farm and Subbasin are
     *               built using the same functions.
     * @param out: Output Statement Call.
     * @param tblA: yield_historic SQL Table.
     * @param tblB: field_farm SQL Table.
     */
    
    private static void fillCropEconFarms(Statement inFld, Statement inBmp, Statement out, String tblA, String tblB){
        try {
            ResultSet inRsFld = inFld.executeQuery("SELECT * FROM " + tblA + ";");
            ResultSet inRsFrm = inBmp.executeQuery("SELECT * FROM " + tblB + " WHERE farm > 0 ORDER BY farm;");
            ResultSet outRs = out.executeQuery("SELECT * FROM " + tbl_names[0] + ";");
            NameTypePair[] ntp = getInputNamesAndTypes(inRsFld);
            String outColumnNames = loadOutputSqlColumnNames(outRs);
            String sql = "INSERT INTO " + tbl_names[0] + "(" + outColumnNames + "VALUES(";
            loadFarmSubbasinTableData(inRsFrm, inFld, tblA, ntp, sql, out, "farm");
        } catch(SQLException e) {
            Logger.getLogger(BuildDB.class.getName()).log(Level.SEVERE, null, e);
        }
    }
    
    /**
     * 
     * @param inFld: Input Statement for the yield_historic SQL Table.
     * @param inBmp: Input Statement for the field_subbasin SQL Table.
     * @param out: Output Statement Call.
     * @param tblA: yield_historic SQL Table.
     * @param tblB: field_subbasin SQL Table.
     */
    
    private static void fillCropEconSubbasins(Statement inFld, Statement inBmp, Statement out, String tblA, String tblB) {
        try {
            ResultSet inRsFld = inFld.executeQuery("SELECT * FROM " + tblA + ";");
            ResultSet inRsBsn = inBmp.executeQuery("SELECT * FROM " + tblB + " WHERE subbasin > 0 ORDER BY subbasin;");
            ResultSet outRs = out.executeQuery("SELECT * FROM " + tbl_names[2] + ";");
            NameTypePair[] ntp = getInputNamesAndTypes(inRsFld);
            String outColumnNames = loadOutputSqlColumnNames(outRs);
            String sql = "INSERT INTO " + tbl_names[2] + "(" + outColumnNames + "VALUES(";
            loadFarmSubbasinTableData(inRsBsn, inFld, tblA, ntp, sql, out, "subbasin");
        } catch(SQLException e) {
            Logger.getLogger(BuildDB.class.getName()).log(Level.SEVERE, null, e);
        }
    }
    
    /**
     * 
     * @param iRs: Input ResultSet containing the query result from the source
     *             table. All data will be written to the output Table using the
     *             out Connection Statement variable. The naming convention for
     *             the variables does not need the 'i' to represent input within
     *             the functions individually. This has been done to ensure that
     *             the documentation for the program can be clearly followed
     *             when troubleshooting bugs in the future. An 'i' prefix
     *             represents a ResultSet using a query with an Input Connection
     *             and an 'o' prefix represents a ResultSet using a query with
     *             an Output Connection.
     * @return ntp: The NameTypePair array compiled in the function. 
     */
    
    private static NameTypePair[] getInputNamesAndTypes(ResultSet iRs) {
        NameTypePair[] ntp = null;
        try {
            ntp = new NameTypePair[iRs.getMetaData().getColumnCount()];
            for(int i = 1; i <= iRs.getMetaData().getColumnCount(); i++) {
                if (iRs.getMetaData().getColumnType(i) == 4) {
                    ntp[i - 1] = new NameTypePair(iRs.getMetaData().getColumnName(i), 1);
                } else {
                    ntp[i - 1] = new NameTypePair(iRs.getMetaData().getColumnName(i), 0);
                }
            }
        } catch (SQLException e) {
            Logger.getLogger(BuildDB.class.getName()).log(Level.SEVERE, null, e);
        }
        return ntp;
    }
    
    /**
     * 
     * @param oRs: Output ResultSet containing the query result from the target table.
     * @return outColumnNames: A formatted String containing all of the column
     *                         names in the table. This is done separately from
     *                         data retrieval in the setOutputValues function to
     *                         allow the columns names of the output tables to
     *                         work with the data from the source tables, based
     *                         on consistent naming standards. 
     * 
     */
    
    private static String loadOutputSqlColumnNames(ResultSet oRs) {
        String outColumnNames = "";
        try {
            String[] outColumnNamesArray = new String[oRs.getMetaData().getColumnCount()];      
            for(int i = 1; i <= oRs.getMetaData().getColumnCount(); i++) {
                outColumnNamesArray[i - 1] = oRs.getMetaData().getColumnName(i);
                if(i == oRs.getMetaData().getColumnCount()) {
                    outColumnNames += outColumnNamesArray[i - 1] + ") ";
                }
                else {
                    outColumnNames += outColumnNamesArray[i - 1] + ", ";
                }
            }
        } catch (SQLException e){
            Logger.getLogger(BuildDB.class.getName()).log(Level.SEVERE, null, e);
        }
        return outColumnNames;
    }
    
    /**
     * 
     * @param rsBmp: ResultSet for either the field_farm or field_subbasin
     *               SQL Table. Due to the same functions being needed for both
     *               the crop_economic_farms and crop_economic_subbasins SQL
     *               Tables, BMP is used as a common identifier in the function.
     * @param in: Input Statement Call.
     * @param tbl: yield_historic SQL Table.
     * @param ntp: The NameTypePair array from getInputNamesAndTypes.
     * @param s: String containing the SQL Query to be written to the output Table.
     * @param out: Output Statement Call.
     */
    
    private static void loadFarmSubbasinTableData(ResultSet rsBmp, Statement in, String tbl, NameTypePair[] ntp, String s, Statement out, String bmp) {
        try {
            int multiField = 0;
            while(rsBmp.next()) {
                String sql = s;
                sql += rsBmp.getInt(2) + ", ";
                int fieldId = rsBmp.getInt(1);
                int bmpId = rsBmp.getInt(2);
                if(multiField == bmpId) {
                }
                else {
                    multiField = bmpId;
                    if(rsBmp.getDouble(3) == 1.0) {
                        ResultSet rsField = in.executeQuery("SELECT * FROM " + tbl + " WHERE field = " + fieldId + ";");
                        while(rsField.next()) {
                            out.executeUpdate(buildFieldOutputQuery(rsField, ntp, sql, 1));
                        }
                    }
                    else {
                        ResultSet rsField = in.executeQuery("SELECT * FROM " + rsBmp.getMetaData().getTableName(1) + " WHERE " + bmp + " = " + rsBmp.getInt(2) + ";");
                        IdPercentPair[] ipp = null, temp = null;
                        int index = 0;
                        while(rsField.next()) {
                            if(ipp == null) {
                                ipp = new IdPercentPair[1];
                                temp = new IdPercentPair[1];
                                ipp[index] = new IdPercentPair(rsField.getInt("field"), rsField.getDouble("percent"));
                                temp[index] = new IdPercentPair();
                                index++;
                            }
                            else {
                                temp = new IdPercentPair[ipp.length];
                                System.arraycopy(ipp, 0, temp, 0, temp.length);
                                ipp = new IdPercentPair[temp.length + 1];
                                System.arraycopy(temp, 0, ipp, 0, temp.length);
                                ipp[index] = new IdPercentPair(rsField.getInt("field"), rsField.getDouble("percent"));
                                index++;
                            }
                        }
                        double[][] bmpPercentTotals = new double[20][5];
                        for (IdPercentPair ipp1 : ipp) {
                            int i = 0;
                            rsField = in.executeQuery("SELECT * FROM " + tbl + " WHERE field = " + ipp1.getPairId() + ";");
                            while(rsField.next()) {
                                try {
                                    for(int j = 1; j < rsField.getMetaData().getColumnCount(); j++) {
                                        if(j == 1) {
                                            bmpPercentTotals[i][j - 1] = rsField.getInt(ntp[j].getPairName());
                                        }
                                        else {
                                            bmpPercentTotals[i][j - 1] += rsField.getDouble(ntp[j].getPairName()) * ipp1.getPairPercent();
                                        }

                                    }
                                } catch(SQLException e) {
                                    Logger.getLogger(BuildDB.class.getName()).log(Level.SEVERE, null, e);
                                }
                                i++;
                            }
                        }
                        for(int x = 0; x < 20; x++) {
                            String pctSql = sql;
                            for(int y = 0; y < 5; y++) {
                                if(y == 0) {
                                    int year = (int) bmpPercentTotals[x][y];
                                    pctSql += year + ", ";
                                }
                                else {
                                    if(y == 4) {
                                        pctSql += bmpPercentTotals[x][y] + ");";
                                    }
                                    else {
                                        pctSql += bmpPercentTotals[x][y] + ", ";
                                    }
                                }
                            }
                            out.executeUpdate(pctSql);
                        }
                    }
                }
            }
        } catch(SQLException e) {
            Logger.getLogger(BuildDB.class.getName()).log(Level.SEVERE, null, e);
        }
    }
    
    /**
     * 
     * @param iRs: Input ResultSet containing the query result from the source table.
     * @param ntp: The NameTypePair array from getInputNamesAndTypes. 
     * @param s: String containing the SQL Query to be written to the output Table.
     * @param n: Starts building the Query String from the column n - 1 in ntp.
     * @return sql: The Query needed for the output statement.
     */
    
    private static String buildFieldOutputQuery(ResultSet iRs, NameTypePair[] ntp, String s, int n) {
        try {
            String sql = s;
            for(int i = n; i < iRs.getMetaData().getColumnCount(); i++) {
                if(ntp[i].getPairType() == 1) {
                    sql += iRs.getInt(ntp[i].getPairName());
                }
                else {
                    sql += iRs.getDouble(ntp[i].getPairName());
                }
                if(i == (iRs.getMetaData().getColumnCount() - 1)) {
                    sql += ");";
                }
                else {
                    sql += ", ";
                }
            }
            return sql;
        } catch(SQLException e) {
            Logger.getLogger(BuildDB.class.getName()).log(Level.SEVERE, null, e);
        }
        return "";
    }
    
    /**
     * @param tbl: Input DBF Formatted Table.
     * @param existing: The String used to mark the Existing Column. Used specifically
     *                  for the base scenarios. A modification will be needed for the
     *                  user-defined scenarios. Possibly a boolean call.
     * @param columns: Column names found within the input Table.
     * @return vals: A collection of the entries for each row in the input Table.
     *               Each row contains the data from the corresponding columns,
     *               as well as the type for the data value as a ValueTypePair.
     *               This is to accommodate potential issues with input data
     *               being read as an integer value instead of a double value.
     *               Pairing the data value with a static type value will prevent
     *               complications with writing the values to the SQLite databases.
     * @throws IOException: File I/O Error when verifying existence of a DBF table.
     */
    
    private static ValueTypePair[][] loadDbfTables(Table tbl, String existing, String[] columns) throws IOException {
        ValueTypePair[][] vals = null;
        try {
            tbl.open(IfNonExistent.ERROR);
            vals = new ValueTypePair[tbl.getRecordCount()][columns.length];
            System.out.println("\nOpened " + tbl.getName() + " database successfully\n");
            
            Iterator<Record> iter = tbl.recordIterator();
            for(int i = 0; i < tbl.getRecordCount(); i++) {
                Record rec = iter.next();
                Number exists = rec.getNumberValue(existing);
                if(exists.intValue() == 1) {
                    for(int j = 0; j < columns.length; j++) {
                        Number n = rec.getNumberValue(columns[j]);
                        vals[i][j] = new ValueTypePair(n.doubleValue(), 0);
                        System.out.println((int) rec.getTypedValue(columns[j]) + ": OBJECT TYPE NUMBER");
                        System.out.printf("%.2f", vals[i][j]);
                        System.out.println(": " + columns[j]);
                    }
                    System.out.println();
                }
            }
        } catch (CorruptedTableException e) {
            Logger.getLogger(BuildDB.class.getName()).log(Level.SEVERE, null, e);
        }
        finally {
            tbl.close();
        }
        return vals;
    }
    
    /**
     * 
     * @param src: An array containing the Strings with the SQLite column names. 
     * @return sql: The partial SQL Query String. 
     */
    
    private static String loadOutputDbfColumnNames(String[] src) {
        String sql = "";
        for(int i = 0; i < src.length; i++) {
            sql += src[i];
            if(i == src.length - 1) {
                sql += ")";
            }
            else {
                sql += ", ";
            }
        }
        return sql;
    }
    
    /**
     * 
     * @param src
     * @param s
     * @return sql: The complete SQL Query String.
     */
    
    private static String buildOutputDbfQueries(ValueTypePair[] src, String s) {
        String sql = s;
        try {
            for(int i = 0; i < src.length; i++) {
                if(src[i].getPairValueAsDouble() != 0) {
                    if(i == src.length - 1) {
                        if(src[i].getPairValueAsInt() != -1) {
                            sql += src[i].getPairValueAsInt() + ")";
                        }
                        else {
                            sql += src[i].getPairValueAsDouble() + ")";
                        }
                    }
                    else {
                        if(src[i].getPairValueAsInt() != -1) {
                            sql += src[i].getPairValueAsInt() + ",";
                        }
                        else {
                            sql += src[i].getPairValueAsDouble() + ",";
                        }
                    }
                }
            }
            return sql;
        } catch(Exception  e) {
            Logger.getLogger(BuildDB.class.getName()).log(Level.SEVERE, null, e);
        }
        return "";
    }
    
    /**
     * @param args
     * @throws ClassNotFoundException: Missing the Library for SQLite version of JDBC.
     * @throws SQLException: Error with Queries made using SQLite.
     * @throws IOException: File I/O Error when verifying existence of a DBF table.
     */
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        Class.forName("org.sqlite.JDBC");

        Connection cInDb3 = DriverManager.getConnection("jdbc:sqlite:" + inDB);
        cInDb3.setAutoCommit(false);
        System.out.println("\nOpened " + inDB + " database successfully");

        Connection cOutput = DriverManager.getConnection("jdbc:sqlite:" + hist);
        cOutput.setAutoCommit(false);
        System.out.println("\nConnected established to " + hist + " database successfully");
        
        try {
            Statement inStmtA = cInDb3.createStatement();
            Statement inStmtB = cInDb3.createStatement();
            Statement outStmt = cOutput.createStatement();

            boolean isDouble = false;
            int index;
            
            buildTables(inStmtA, outStmt);
            System.out.println("\n" + hist + " database created successfully");
            
            //float num = (float) (4.87e-7 * Math.pow(nv.intValue(), 3.0) - 4.24e-3 * Math.pow(nv.intValue, 2.0) + 1.28e1 * 2682 + 6.71e3);
            //System.out.println(num / 50);
            
            String tbl = "yield_historic";
            fillCropEconFields(inStmtA, outStmt, tbl);
            System.out.println("\ncrop_economic_fields database created successfully");
            String tblB = "field_farm";
            fillCropEconFarms(inStmtA, inStmtB, outStmt, tbl, tblB);
            System.out.println("\ncrop_economic_farms database created successfully");
            tblB = "field_subbasin";
            fillCropEconSubbasins(inStmtA, inStmtB, outStmt, tbl, tblB);
            System.out.println("\ncrop_economic_subbasins database created successfully");
            
            ValueTypePair[][] src;
            String[] dam = {"ID", "Embankment", "LifeTime"};
            src = loadDbfTables(new Table(new File(dbf_tbls[0].getAbsolutePath())), "Existing", dam);
            
            for(ValueTypePair[] s: src) {
                String sql = "INSERT INTO " + tbl_names[13] + "(";
                String[] sqlNames = {"id", "embankment", "life_time"};
                sql += loadOutputDbfColumnNames(sqlNames) + " VALUES(";
                outStmt.executeUpdate(buildOutputDbfQueries(s, sql));
            }
            
            for(ValueTypePair[] s: src) {
                String sql = "INSERT INTO " + tbl_names[9] + "(";
                String[] sqlNames = {"id", "embankment", "life_time"};
                sql += loadOutputDbfColumnNames(sqlNames) + " VALUES(";
                outStmt.executeUpdate(buildOutputDbfQueries(s, sql));
            }
            
            String[] pond = {"ID", "HRU", "Cattles", "ClayLiner", "PlasticLn", "WireFence", "Distance", "Trenching", "Pond_Yrs"};
            src = loadDbfTables(new Table(new File(dbf_tbls[1].getAbsolutePath())), "Existing", pond);
            
            for(ValueTypePair[] s: src) {
                for(int i = 0; i < s.length; i++) {
                    try {
                        if(s[i].getPairValueAsDouble() != 0) {
                            if(s[i].getPairValueAsInt() != -1) {
                                int val = s[i].getPairValueAsInt();
                                System.out.println(val);
                            }
                            else {
                                double val = s[i].getPairValueAsDouble();
                                System.out.println(val);
                            }
                            if(i == s.length - 1) {
                                System.out.println();
                            }
                        }
                    }
                    catch(NumberFormatException e) {
                        System.out.println(e);
                    }
                }
            }
            
            String[] graze = {"ID", "Grazing_Ha", "UnitCost"};
            src = loadDbfTables(new Table(new File(dbf_tbls[2].getAbsolutePath())), "Existing", graze);
            
            for(ValueTypePair[] s: src) {
                for(int i = 0; i < s.length; i++) {
                    try {
                        if(s[i].getPairValueAsDouble() != 0) {
                            if(s[i].getPairValueAsInt() != -1) {
                                int val = s[i].getPairValueAsInt();
                                System.out.println(val);
                            }
                            else {
                                double val = s[i].getPairValueAsDouble();
                                System.out.println(val);
                            }
                            if(i == s.length - 1) {
                                System.out.println();
                            }
                        }
                    }
                    catch(NumberFormatException e) {
                        System.out.println(e);
                    }
                }
            }
                /*
                int i = 0;
                while(i < 26) {
                String s1 = "Embankment";
                String s2 = "LifeTime";
                Record rec = iter.next();
                double num = 0;
                Number n1 = rec.getNumberValue(s1);
                Number n2 = rec.getNumberValue(s2);

                if(n1 == null) {
                System.out.println("EMPTY");
                }
                else if(n1.doubleValue() % 1 == 0 && !isDouble) {
                System.out.println(n1.intValue() + ": TYPE = INT");
                num = (4.87e-7 * Math.pow(n1.intValue(), 3.0) - 4.24e-3 * Math.pow(n1.intValue(), 2.0) + 1.28e1 * 2682 + 6.71e3);
                System.out.printf("%.2f", num / 50);
                System.out.println();
                }
                else {
                isDouble = true;
                System.out.println(n1 + ": TYPE = DOUBLE");
                }
                if(n2 == null) {
                System.out.println("EMPTY");
                }
                else if(n2.doubleValue() % 1 == 0 && !isDouble) {
                System.out.println(n2.intValue() + ": TYPE = INT");
                num = (4.87e-7 * Math.pow(n2.intValue(), 3.0) - 4.24e-3 * Math.pow(n2.intValue(), 2.0) + 1.28e1 * 2682 + 6.71e3);
                System.out.println(num / 50);
                System.out.println(n2);
                }
                else {
                isDouble = true;
                System.out.println(n2 + ": TYPE = DOUBLE");
                }
                i++;
                }
                */
                
                /** HOLDING POND MAINTENANCE COST
                 *                 double temp = 0.03048 * Math.Pow(Math.Sqrt(1.68 * Cattles) - 6,2.0);
                 * double min = 1.38e-10 * Math.Pow(temp, 2.0)
                 * - 5.027e-5 * temp
                 * + 6.737
                 * + _wire_fence * (24.48 + 3.05 * Math.Sqrt(Cattles))
                 * + 1.25 * Cattles;
                 * 
                 * temp = 0.03048 * Math.Pow(Math.Sqrt(2.48 * Cattles) - 6, 2.0);
                 * double max = 1.38e-10 * Math.Pow(temp, 2.0)
                 * - 5.027e-5 * temp
                 * + 6.737
                 * + _wire_fence * (24.48 + 3.71 * Math.Sqrt(Cattles))
                 * + 1.85 * Cattles;
                 * 
                 * return min / 2.0 + max / 2.0;
                 */
                // HOLDING POND TOTALCOST = ANNUAL + MAINTENANCE
                // HOLDING_ECON COST == TOTALCOST
                
                // GRAZING COST = UNIT COST * AREA from DBF
            cOutput.commit();
        } catch(SQLException e) {
            Logger.getLogger(BuildDB.class.getName()).log(Level.SEVERE, null, e);
        } finally {
            cOutput.close();
            cInDb3.close();
        }
    }
}