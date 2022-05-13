import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static java.lang.System.exit;

public class DataLoader {
    private Connection conn;

    public boolean connected = false;
    private String addr = "127.0.0.1:3306";
    private String db = "OpenStreetMap";
    String user = "osmap";
    String pass = "osmapassword";
    String urlPrefix = "jdbc:mysql://";
    public DataLoader(){

    }
    public boolean openConnection() {
        try {
            if (conn == null || conn.isClosed()) {
                // String timezone = "?serverTimezone=+00:00";
                String url = urlPrefix + addr + "/" + db;
                // url = url + timezone;
                conn = DriverManager.getConnection(url, user, pass);
                connected = true;

            }
        } catch (SQLException e) {
            System.err.println("Error al abrir la conexion: ");
            System.err.println(e.getMessage());
            return false;
        }
        return true;
    }

    public boolean createDB(){
        String url = urlPrefix + addr;
        boolean res = true;
        Connection connection = null;
        try{
            connection = DriverManager.getConnection(url, user, pass);
            Statement stmt = connection.createStatement();
            String sql = "CREATE DATABASE IF NOT EXISTS OpenStreetMap";
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            res = false;
        }
        finally {
            if(connection != null)
                try {
                    connection.close();
                } catch (SQLException e) {
                    System.err.println("Error al cerrar la conexión: ");
                }
        }
        return res;
    }

    public boolean closeConnection() {
        try {
            conn.close();
            connected = false;
        } catch (SQLException e) {
            System.err.println("Error al cerrar la conexión: ");
            System.err.println(e.getMessage());
        }
        return true;
    }

    public boolean crateAllNodesTable() {
        Statement st = null;
        try {
            String sql = "CREATE TABLE IF NOT EXISTS allNodes ("
                    + "  id BIGINT,"
                    + "  lon DOUBLE,"
                    + "  lat DOUBLE,"
                    + "  name VARCHAR(300),"
                    + "  opening_hours VARCHAR(300),"
                    + "  PRIMARY KEY (id));";

            this.openConnection();

            st = conn.createStatement();
            st.executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return false;
        } finally {
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
        return true;

    }

    public boolean createCuisineTable() {
        Statement st = null;
        try {
            String sql = "CREATE TABLE IF NOT EXISTS cuisine ("
                    + "  id BIGINT,"
                    + "  lon DOUBLE,"
                    + "  lat DOUBLE,"
                    + "  name VARCHAR(300),"
                    + "  opening_hours VARCHAR(300),"
                    + "  phone VARCHAR(15),"
                    + "  website VARCHAR(200),"
                    + "  PRIMARY KEY (id));";

            this.openConnection();

            st = conn.createStatement();
            st.executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return false;
        } finally {
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
        return true;

    }

    public boolean loadNodos() {
        boolean res = true;
        this.openConnection();
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Error de conexion al desactivar el AutoComit" + e.getMessage());
            return false;
        }
        List<List<String>> data = readFromCSV();
        PreparedStatement pst = null;
        String query = "INSERT INTO  allNodes  "
                + "(id, lon, lat, name, opening_hours) VALUES (?,?,?,?,?);";

        try {
            pst = conn.prepareStatement(query);
            for (int i = 1; i < data.size(); i++) {
                pst.setLong(1, Long.parseLong(data.get(i).get(0)));
                pst.setDouble(2, Double.parseDouble(data.get(i).get(1)));
                pst.setDouble(3, Double.parseDouble(data.get(i).get(2)));
                pst.setString(4, data.get(i).get(3));
                pst.setString(5, data.get(i).get(4));
                pst.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            System.err.println("Error SQL cargar los datos de aprende en la base de datos: " + e.getMessage());
            res = false;
        } finally {
            try {
                if (pst != null)
                    pst.close();
            } catch (SQLException e1) {
                System.err.println("Error al cerrar un PreparedStatement: " + e1.getMessage());
            }
        }
        return res;
    }

    public boolean loadCuisine() {
        boolean res = true;
        this.openConnection();
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Error de conexion al desactivar el AutoComit" + e.getMessage());
            return false;
        }
        List<List<String>> data = readFromCSV();
        PreparedStatement pst = null;
        String query = "INSERT INTO  cuisine  "
                + "(id, lon, lat, name, opening_hours, phone, website) VALUES (?,?,?,?,?,?,?);";

        try {
            pst = conn.prepareStatement(query);
            for (int i = 1; i < data.size(); i++) {
                pst.setLong(1, Long.parseLong(data.get(i).get(0)));
                pst.setDouble(2, Double.parseDouble(data.get(i).get(1)));
                pst.setDouble(3, Double.parseDouble(data.get(i).get(2)));
                pst.setString(4, data.get(i).get(3));
                pst.setString(5, data.get(i).get(4));
                pst.setString(6, data.get(i).get(4));
                pst.setString(7, data.get(i).get(4));
                pst.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            System.err.println("Error SQL cargar los datos de aprende en la base de datos: " + e.getMessage());
            res = false;
        } finally {
            try {
                if (pst != null)
                    pst.close();
            } catch (SQLException e1) {
                System.err.println("Error al cerrar un PreparedStatement: " + e1.getMessage());
            }
        }
        return res;
    }
    private List<List<String>> readFromCSV() {
        List<List<String>> out = new ArrayList<>();
        InputStreamReader isReader = new InputStreamReader(System.in);
        BufferedReader bufReader = new BufferedReader(isReader);
        String line;
        while (true) {
            try {
                if ((line = bufReader.readLine()) == null) break;
            } catch (IOException e) {
                break;
            }
            String[] values = line.split(";");
            out.add(Arrays.asList(values));
        }
        return out;
    }

    public static void main(String[] args) {
        DataLoader dataLoader = new DataLoader();
        int status = 0;
        if(args.length == 0)
            status = dataLoader.createDB() && dataLoader.crateAllNodesTable() && dataLoader.loadNodos() ? 0 : -1;
        else if (args.length == 1 && args[0].equals("--cuisine"))
            status = dataLoader.createDB() && dataLoader.createCuisineTable() && dataLoader.loadCuisine() ? 0 : -1;
        else
            System.out.println("USO: [--cusine]");
        if (dataLoader.connected)
            dataLoader.closeConnection();
        exit(status);
    }
}