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

import java.sql.*;

/**
 *
 * @author Alan Snelson
 */
public class MySQL {

    private Connection conn = null;
    private String hostname;
    private int port;
    private String username;
    private String password;

    public MySQL(String hostname, int port, String username, String password) throws SQLException {
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
    private void connect(String hostname, int port, String username, String password, String db) throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection("jdbc:mysql://" + hostname + ":" + port + "/" + db, username, password);
        } catch (SQLException se) {
            throw se;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    public void executeScript(String schema, String sqlstmt) throws SQLException {
        try {
            conn.setCatalog(schema);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sqlstmt);
        } catch (SQLException sqle) {
        }
    }

    
}