package dbTest;

import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;

public class dbTest {
    
    // In the WEBsInterface, Pass these as parameters.
    
    private static final String inDB = "spatial";
    private static final String outDB = "base_historic";
    
    public static void main(String args[]) throws ClassNotFoundException, SQLException {
        
        Connection cInput = null, cOutput = null;
        Statement inStmt = null, outStmt = null;
        ArrayList<String> tables, headers, types;
        int index;
        try {
            Class.forName("org.sqlite.JDBC");
            cInput = DriverManager.getConnection("jdbc:sqlite:" + inDB + ".db3");
            cInput.setAutoCommit(false);
            inStmt = cInput.createStatement();
            System.out.println("Opened " + inDB + " database successfully");
            
            cOutput = DriverManager.getConnection("jdbc:sqlite:" + outDB + ".db3");
            cOutput.setAutoCommit(false);
            outStmt = cOutput.createStatement();
            System.out.println("\nOpened " + outDB + " database successfully");
            
            ResultSet prs = inStmt.executeQuery("SELECT * FROM sqlite_master WHERE type = 'table';");
            tables = new ArrayList();
            
            while(prs.next()) {
                tables.add(prs.getString(3));
            }
            
            index = 0;
            headers = new ArrayList();
            types = new ArrayList();
            for(String s : tables) {
                String numType;
                prs = inStmt.executeQuery("PRAGMA table_info(" + s + ")");
                while(prs.next()) {
                    for(int i = 1; i < prs.getMetaData().getColumnCount(); i++) {
                        try {
                            String entry = prs.getString(i);
                            Scanner sc = new Scanner(entry);
                            if(sc.hasNextDouble() || sc.hasNextInt()) {}
                            else if(entry.equalsIgnoreCase("INT")) {
                                numType = "int";
                                types.add(numType);
                                index++;
                            }
                            else if(entry.equalsIgnoreCase("REAL")) {
                                numType = "real";
                                types.add(numType);
                                index++;
                            }
                            else if(entry.equalsIgnoreCase("TEXT")) {
                                numType = "text";
                                types.add(numType);
                                index++;
                            }
                            else {
                                if(entry.equalsIgnoreCase("HRUSWATIndex")) {
                                    headers.add("hru_swat_index");
                                }
                                else if(entry.equalsIgnoreCase("netreturn")) {
                                    headers.add("net_return");
                                }
                                else if(entry.equalsIgnoreCase("hrufile")) {
                                    headers.add("hru_file");
                                }
                                else {
                                headers.add(entry);
                                }
                            }
                        }
                        catch(NullPointerException e) {
                            //System.err.println(e.getClass().getName() + ": " + e.getMessage());
                        }
                    }
                    index++;
                }
                headers.add("EoE");
                types.add("EoE");
            }
            
            index = 0;
            String headSql = "", instSql = "", dataSql = "";
            boolean tblMissing = true, eoeMissing;
            
            for (String s: tables) {
                eoeMissing = false;
                ResultSet rs = inStmt.executeQuery("SELECT * FROM " + s + ";");
                if(tblMissing) {
                    headSql = "CREATE TABLE IF NOT EXISTS " + s + "(";
                    instSql = "INSERT INTO " + s + "(";
                    dataSql = " VALUES(";
                    tblMissing = false;
                }

                if(headers.get(index).equalsIgnoreCase("EoE")) {
                    System.out.println("\nFOUND EoE");
                }
                else {
                    while(!eoeMissing) {
                        System.out.println("\nTABLE: " + s);
                        System.out.println("\nHEADER: " + headers.get(index));
                        System.out.println("\nTYPES: " + types.get(index));
                        int dataIndex = 0;
                        while(rs.next()) {
                            if(headers.get(index + 1).equalsIgnoreCase("EoE")) {
                                headSql = headSql + headers.get(index) + " " + types.get(index) + ");";
                                instSql = instSql + headers.get(index) + ") ";

                                if(types.get(index).equalsIgnoreCase("int")) {
                                    int data;
                                    if(headers.get(index).equalsIgnoreCase("hru_swat_index")) {
                                        data = rs.getInt("HRUSWATIndex");
                                        dataIndex++;
                                    }
                                    else {
                                        data = rs.getInt(headers.get(dataIndex));
                                        dataIndex++;
                                    }
                                    dataSql = dataSql + data + ");";
                                }
                                else if(types.get(index).equalsIgnoreCase("real")) {
                                    double data;
                                    if(headers.get(index).equalsIgnoreCase("net_return")) {
                                        data = rs.getDouble("netreturn");
                                        dataIndex++;
                                    }
                                    else {
                                        data = rs.getDouble(headers.get(index));
                                        dataIndex++;
                                    }
                                    dataSql = dataSql + data + ");";
                                }
                                else {
                                    String data;
                                    if(headers.get(index).equalsIgnoreCase("hru_file")) {
                                        data = "'" +rs.getString("hrufile") + "'";
                                        dataIndex++;
                                    }
                                    else {
                                        data = rs.getString(headers.get(index));
                                        dataIndex++;
                                    }
                                    dataSql = dataSql + data  + ");";
                                }

                                System.out.println("\n" + headSql);
                                outStmt.executeUpdate(headSql);
                                System.out.println("\nTABLE PUSHED\n");

                                eoeMissing = true;
                            }
                            else {
                                headSql = headSql + headers.get(index) + " " + types.get(index) + ", ";
                                instSql = instSql + headers.get(index) + ", ";

                                if(types.get(index).equalsIgnoreCase("int")) {
                                    int data;
                                    if(headers.get(index).equalsIgnoreCase("hru_swat_index")) {
                                        data = rs.getInt("HRUSWATIndex");
                                        dataIndex++;
                                    }
                                    else {
                                        data = rs.getInt(headers.get(index));
                                        dataIndex++;
                                    }
                                    dataSql = dataSql + data + ", ";
                                }
                                else if(types.get(index).equalsIgnoreCase("real")) {
                                    double data;
                                    if(headers.get(index).equalsIgnoreCase("net_return")) {
                                        data = rs.getDouble("netreturn");
                                        dataIndex++;
                                    }
                                    else {
                                        data = rs.getDouble(headers.get(index));
                                        dataIndex++;
                                    }
                                    dataSql = dataSql + data + ", ";
                                }
                                else {
                                    String data;
                                    if(headers.get(index).equalsIgnoreCase("hru_file")) {
                                        data = "'" + rs.getString("hrufile") + "'";
                                        dataIndex++;
                                    }
                                    else {
                                        data = "'" + rs.getString(headers.get(index)) + "'";
                                        dataIndex++;
                                    }
                                    dataSql = dataSql + data + ", ";
                                }
                            }
                        }
                    }
                }
                instSql = instSql + dataSql;
                System.out.println(instSql);
                outStmt.executeUpdate(instSql);
                index++;
                tblMissing = true;
            }
            cOutput.commit();
        } catch (SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        finally {
            inStmt.close();            
            outStmt.close();
            cInput.close();
            cOutput.close();
        }
        System.out.println("\nOperation done successfully");
    }
}