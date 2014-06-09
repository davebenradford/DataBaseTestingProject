package dbTest;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import nl.knaw.dans.common.dbflib.*;

public class buildDB {
    private static final String hist = "base_historic";
    private static final String conv = "base_conventional";
    private static final String inDB = "spatial";
    private static final String inDBF = "farm2010";
    private static final String[] tbl_names = {"crop_economic_farms", "crop_economic_fields", "crop_economic_subbasins", "forage", "forage_hru", "grazing",
                                               "grazing_economic", "grazing_economic_subbasins", "grazing_hrus", "holding_ponds", "holding_ponds_economic",
                                               "small_dams", "small_dams_economic", "tillage", "tillage_hrus"};
    private static final String[] val_names = {"id", "year", "yield", "revenue", "cost", "net_return", "grazing_ha", "unit_cost", "hru", "cattle",
                                               "clay_liner", "plastic_ln", "wire_fence", "distance", "trenching", "pond_yrs", "annual_cost",
                                               "maintenance", "total_cost", "embankment", "life_time", "tillage"};
    private static final String[] types = {"int", "real"};
    private static final String[] dbf_tbls = {"small_dam", "cattle_yard", "grazing", "land2010_by_land_id", "farm2010"};
    
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
                    System.out.println("\n" + sql);
                    out.executeUpdate(sql);
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(buildDB.class.getName()).log(Level.SEVERE, null, ex);
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
     * @param tbl: Name of the Input Table from the SQLite3 DB.  
     */
    
    private static void fillCropEconFields(Statement in, Statement out, String tbl) {
        try {
            ResultSet inRs = in.executeQuery("SELECT * FROM " + tbl + ";");
            ResultSet outRs = out.executeQuery("SELECT * FROM " + tbl_names[1] + ";");
            NameTypePair[] ntp = getInNamesAndTypes(inRs, false);
            String outColumnNames = loadOutputColumnNames(outRs);
            setOutputValues(inRs, ntp, outColumnNames, out);
        } catch (SQLException e) {
            Logger.getLogger(buildDB.class.getName()).log(Level.SEVERE, null, e);
        }
    }
    
    private static void fillCropEconFarm(Statement in, Statement out, String tblA, String tblB){
        try {
            ResultSet inRsFrm = in.executeQuery("SELECT DISTINCT farm FROM " + tblB + "WHERE farm > 0;");
            ResultSet inRsFld = in.executeQuery("SELECT * FROM " + tblA + ";");
            ResultSet outRs = out.executeQuery("SELECT * FROM " + tbl_names[0] + ";");
            //NameTypePair[] ntp = getInNamesAndTypes(inRsFld, true);
            String[] inColumnNames = new String[inRsFld.getMetaData().getColumnCount()];
            String[] inColumnPos = new String[inRsFld.getMetaData().getColumnCount()];
            inColumnNames[0] = inRsFrm.getMetaData().getColumnName(2);
            inColumnPos[0] = "int";
            for(int i = 2; i <= inRsFld.getMetaData().getColumnCount(); i++) {
                inColumnNames[i - 1] = inRsFld.getMetaData().getColumnName(i);
                if(inRsFld.getMetaData().getColumnType(i) == 4) {
                    inColumnPos[i - 1] = "int";
                }
                else {
                    inColumnPos[i - 1] = "dbl";
                }
            }
            //String outColumnNames = loadOutputColumnNames(outRs);
            //setOutputValues(inRs, ntp, outColumnNames, out);
        } catch(SQLException e) {
            Logger.getLogger(buildDB.class.getName()).log(Level.SEVERE, null, e);
        }
    }
    
    private static NameTypePair[] getInNamesAndTypes(ResultSet iRs, boolean hasFirst) {
        NameTypePair[] ntp = null;
        int x = 0;
        try {
            ntp = new NameTypePair[iRs.getMetaData().getColumnCount()];
            if(hasFirst) {
                x = 2;
            }
            else {
                x = 1;
            }
            for(int i = x; i <= iRs.getMetaData().getColumnCount(); i++) {
                if (iRs.getMetaData().getColumnType(i) == 4) {
                    ntp[i - 1] = new NameTypePair(iRs.getMetaData().getColumnName(i), 1);
                } else {
                    ntp[i - 1] = new NameTypePair(iRs.getMetaData().getColumnName(i), 0);
                }
            }
        } catch (SQLException e) {
            Logger.getLogger(buildDB.class.getName()).log(Level.SEVERE, null, e);
        }
        return ntp;
    }
    
    private static String loadOutputColumnNames(ResultSet oRs) {
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
            Logger.getLogger(buildDB.class.getName()).log(Level.SEVERE, null, e);
        }
        return outColumnNames;
    }
    
    private static void setOutputValues(ResultSet iRs, NameTypePair[] ntp, String outCols, Statement out) {
        try {
            while(iRs.next()) {
                String sql = "INSERT INTO " + tbl_names[1] + "(" + outCols + "VALUES(";
                for(int i = 0; i < iRs.getMetaData().getColumnCount(); i++) {
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
                out.executeUpdate(sql);
            }
        } catch(SQLException e) {
            Logger.getLogger(buildDB.class.getName()).log(Level.SEVERE, null, e);
        }
    }
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, CorruptedTableException {
        Connection cInDb3 = null, cOutput = null;
        Statement inStmt = null, outStmt =  null;
        Table table = null;
        boolean isDouble = false;
        int index;
        try {
            Class.forName("org.sqlite.JDBC");
            
            cInDb3 = DriverManager.getConnection("jdbc:sqlite:" + inDB + ".db3");
            cInDb3.setAutoCommit(false);
            inStmt = cInDb3.createStatement();
            System.out.println("\nOpened " + inDB + " database successfully");
            
            cOutput = DriverManager.getConnection("jdbc:sqlite:" + hist + ".db3");
            cOutput.setAutoCommit(false);
            outStmt = cOutput.createStatement();
            System.out.println("\nConnected established to " + hist + " database successfully");
            
            table = new Table(new File("C:\\Users\\radfordd\\Documents\\NetBeansProjects\\DataBaseTestingProject\\Data\\Spatial\\" + dbf_tbls[0] + ".dbf"));
            table.open(IfNonExistent.ERROR);
            System.out.println("\nOpened " + inDBF + " database successfully");
            
            buildTables(inStmt, outStmt);
            System.out.println("\n" + hist + " database created successfully");
            
            /*
            Iterator<Record> iter = table.recordIterator();
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
            System.out.printf("%.2f", num / 50);
            System.out.println(n2);
            }
            else {
            isDouble = true;
            System.out.println(n2 + ": TYPE = DOUBLE");
            }
            i++;
            }
            
            float num = (float) (4.87e-7 * Math.pow(nv.intValue(), 3.0) - 4.24e-3 * Math.pow(nv.intValue, 2.0) + 1.28e1 * 2682 + 6.71e3);
            System.out.println(num / 50);
            */
            
            String tbl = "yield_historic";
            fillCropEconFields(inStmt, outStmt, tbl);
            
            
            /** HOLDING POND COST / Lifetime (50)
                            double sqrtCattles = Math.Sqrt(Cattles);
                double temp3 = 2.232 * Cattles + 11.338 * sqrtCattles;
                double temp = 3.72 * Cattles + _trenching * 7.94 * sqrtCattles + 0.844 * Distance +
                    _clay_liner * temp3;

                double temp2 = (0.5 * 9.5 + 7.47) * temp3;

                double max = 1.38e-10 * Math.Pow(temp,2.0) 
                    - 5.027e-5 * temp 
                    + 6.736 + _clay_liner * temp2
                    + _plastic_liner / 0.7 * temp2
                    + _wire_fence * (189.0 + Math.Sqrt(820.0 * Cattles))
                    + 10000.0;
                max *= 1.1483;

                temp3 = 1.512 * Cattles + 9.332 * sqrtCattles;
                temp = 2.52 * Cattles + _trenching * 6.54 * sqrtCattles + 0.844 * Distance +
                    _clay_liner * temp3;
                temp2 = (0.5 * 9.5 + 7.47) * temp3;
                double min = 1.38e-10 * Math.Pow(temp, 2.0)
                    - 5.027e-5 * temp
                    + 6.736 + _clay_liner * temp2
                    + _plastic_liner / 0.7 * temp2
                    + _wire_fence * (189.0 + Math.Sqrt(556.0 * Cattles))
                    + 10000.0;
                min *= 1.1483;

                return min / 2.0 + max / 2.0; 
                */
            
            /** HOLDING POND MAINTENANCE COST
             *                 double temp = 0.03048 * Math.Pow(Math.Sqrt(1.68 * Cattles) - 6,2.0);
                double min = 1.38e-10 * Math.Pow(temp, 2.0)
                    - 5.027e-5 * temp
                    + 6.737
                    + _wire_fence * (24.48 + 3.05 * Math.Sqrt(Cattles))
                    + 1.25 * Cattles;

                temp = 0.03048 * Math.Pow(Math.Sqrt(2.48 * Cattles) - 6, 2.0);
                double max = 1.38e-10 * Math.Pow(temp, 2.0)
                    - 5.027e-5 * temp
                    + 6.737
                    + _wire_fence * (24.48 + 3.71 * Math.Sqrt(Cattles))
                    + 1.85 * Cattles;

                return min / 2.0 + max / 2.0; 
                */
            // HOLDING POND TOTALCOST = ANNUAL + MAINTENANCE
            // HOLDING_ECON COST == TOTALCOST
            
            // GRAZING COST = UNIT COST * AREA from DBF
        } catch(SQLException e) {
            System.out.println(e);
        } finally {
            table.close();
            cOutput.commit();
            cOutput.close();
        }
    }
}
