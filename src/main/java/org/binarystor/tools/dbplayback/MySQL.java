/**
 * Copyright (c) 2010 Wave2 Limited. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of Wave2 Limited nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.binarystor.tools.dbplayback;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;

/**
 *
 * @author Alan Snelson
 */
public class MySQL implements Database {

    private Connection conn = null;
    private String hostname;
    private int port;
    private String username;
    private String password;

    public MySQL(String hostname, int port, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        connect(hostname, port, username, password, "mysql");
    }

    /**
     * Connect to MySQL server
     *
     * @param  host      MySQL Server Hostname
     * @param  username  MySQL Username
     * @param  password  MySQL Password
     * @param  db        Default database
     */
    private void connect(String hostname, int port, String username, String password, String db) {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection("jdbc:mysql://" + hostname + ":" + port + "/" + db, username, password);
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    @Override
    public boolean checkSchema(String schema) {
        //Ok lets see if the database exists - if not create it
        try {
            conn.setCatalog("INFORMATION_SCHEMA");
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT COUNT(*) AS schema_exists FROM SCHEMATA WHERE SCHEMA_NAME='" + schema + "';");
            ResultSet rs = s.getResultSet();
            rs.next();
            if (rs.getInt("schema_exists") != 1) {
                Statement stmt = conn.createStatement();
                //Create Schema
                stmt.executeUpdate("CREATE DATABASE " + schema);
                stmt.close();
                //Create dbPlayback version table
                createVersionTable(schema);
            }
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
            return false;
        }
        return true;
    }

    private void createVersionTable(String schema) {
        try {
            //TODO consider a better primary key
            String createSQL = "CREATE  TABLE dbPlayback (version INT UNSIGNED NOT NULL ,"
                    + "hostname VARCHAR(255) NOT NULL ,"
                    + "script VARCHAR(255) NOT NULL ,"
                    + "applied TIMESTAMP NOT NULL ,"
                    + "status INT NOT NULL ,"
                    + "message VARCHAR(255) NOT NULL ,"
                    + "PRIMARY KEY (version, applied) );";
            conn.setCatalog(schema);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(createSQL);
            stmt.close();
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
        }
    }

    @Override
    public int getVersion(String schema) {
        int version = 0;
        try {
            conn.setCatalog("INFORMATION_SCHEMA");
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT COUNT(*) AS table_exists FROM TABLES WHERE TABLE_SCHEMA='" + schema + "' AND TABLE_NAME='dbPlayback';");
            ResultSet rs = s.getResultSet();
            rs.next();
            if (rs.getInt("table_exists") == 1) {
                //Version table exists - get latest version
                conn.setCatalog(schema);
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
                stmt.executeQuery("SELECT MAX(version) AS version FROM dbPlayback WHERE status=1;");
                rs = stmt.getResultSet();
                if (rs.next()) {
                    version = rs.getInt("version");
                }
                stmt.close();
            } else {
                //Create missin version table
                createVersionTable(schema);
            }
            s.close();
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
        }
        return version;
    }

    @Override
    public boolean setVersion(String schema, int version, String script, int status, String message) {
        try {
            String insertVersion = "INSERT INTO dbPlayback (version, hostname, script, applied, status, message) VALUES (?,?,?,NOW(),?,?);";
            conn.setCatalog(schema);
            PreparedStatement stmt = conn.prepareStatement(insertVersion);
            stmt.setInt(1, version);
            stmt.setString(2, InetAddress.getLocalHost().getHostAddress());
            stmt.setString(3, script);
            stmt.setInt(4, status);
            stmt.setString(5, message);
            stmt.executeUpdate();
            stmt.close();
        } catch (UnknownHostException uhe) {
            System.err.println(uhe.getMessage());
            return false;
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public String executeScript(String schema, BufferedReader script) {
        String result = "";
        String line;
        StringBuilder sb = new StringBuilder();
        try {
            while ((line = script.readLine()) != null) {
                //Strip standalone comments
                if (!(line.startsWith("--") || line.startsWith("/*") || line.length() == 0)) {
                    sb.append(line);
                }
            }
            conn.setCatalog(schema);
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            for (String sqlstmt : sb.toString().replaceAll("(?i)(^|;)(ALTER|CREATE|DROP|INSERT|RENAME|SET)", "$1ZZZZ$2").split("ZZZZ")) {
                if (!sqlstmt.equals("")) {
                    stmt.addBatch(sqlstmt);
                }
            }
            stmt.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);
            stmt.close();
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        } catch (SQLException sqle) {
            if (conn != null) {
                try {
                    conn.rollback();
                    conn.setAutoCommit(true);
                    return sqle.getMessage();
                } catch (SQLException e) {
                    return e.getMessage();
                }

            }
        }
        return result;
    }
}
