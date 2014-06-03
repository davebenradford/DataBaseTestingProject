package dbTest;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
            /**
            s.executeUpdate("CREATE TABLE IF NOT EXISTS crop_economic_fields(id int, year int, yield real, revenue real, cost real, net_return real);");        // 1
            s.executeUpdate("CREATE TABLE IF NOT EXISTS crop_economic_subbasins(id int, year int, yield real, revenue real, cost real, net_return real);");     // 1
            s.executeUpdate("CREATE TABLE IF NOT EXISTS forage(id int);");                                                                                      // 2
            s.executeUpdate("CREATE TABLE IF NOT EXISTS forage_hru(id int);");                                                                                  // 2
            s.executeUpdate("CREATE TABLE IF NOT EXISTS tillage(id int);");                                                                                     // 2
            s.executeUpdate("CREATE TABLE IF NOT EXISTS grazing_hrus(id int);");                                                                                // 2
            
            s.executeUpdate("CREATE TABLE IF NOT EXISTS holding_ponds_economic(id int, year int, cost real);");                                                 // 3
            s.executeUpdate("CREATE TABLE IF NOT EXISTS grazing_economic(id int, year int, cost real);");                                                       // 3
            s.executeUpdate("CREATE TABLE IF NOT EXISTS grazing_economic_subbasins(id int, year int, cost real);");                                             // 3
            s.executeUpdate("CREATE TABLE IF NOT EXISTS small_dams_economic(id int, year int, cost real);");                                                    // 3
            
            s.executeUpdate("CREATE TABLE IF NOT EXISTS grazing(id int, grazing_ha real, unit_cost real, cost real);");                                         // 4
            s.executeUpdate("CREATE TABLE IF NOT EXISTS holding_ponds(id int, hru int, cattle real, clay_liner int, plastic_ln int, wire_fence int, "
                          + "distance real, trenching real, pond_yrs int, cost real, annual_cost real, maintenance real, total_cost real);");                   // 5
            
            s.executeUpdate("CREATE TABLE IF NOT EXISTS small_dams(id int, embankment real, life_time int);");                                                  // 6
            
            s.executeUpdate("CREATE TABLE IF NOT EXISTS tillage_hrus(id int, tillage int);");                                                                   // 7
            */
        } catch (SQLException ex) {
            Logger.getLogger(buildDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    private static void fillTables(Statement out) {
        try {
            for(String tbl: tbl_names){
                ResultSet rs = out.executeQuery("SELECT * FROM " + tbl + ";");
                System.out.println("\n" + tbl);
            }
        } catch (SQLException ex) {
            Logger.getLogger(buildDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, CorruptedTableException {
        Connection cInDb3 = null, cOutput = null;
        Statement inStmt = null, outStmt =  null;
        Table table = null;
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
            
            table = new Table(new File("C:\\Users\\radfordd\\Documents\\NetBeansProjects\\Data\\Spatial\\farm2010.dbf"));
            table.open(IfNonExistent.ERROR);
            System.out.println("\nOpened " + inDBF + " database successfully");
            
            buildTables(inStmt, outStmt);
            System.out.println("\n" + hist + " database created successfully");
            
            fillTables(outStmt);
        }
        catch(SQLException e) {
            System.out.println(e);
        }
        finally {
            table.close();
            cOutput.commit();
            cOutput.close();
        }
    }
}
