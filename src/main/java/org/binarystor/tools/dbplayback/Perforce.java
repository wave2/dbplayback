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
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import com.perforce.p4java.client.IClient;
import com.perforce.p4java.core.file.FileSpecBuilder;
import com.perforce.p4java.core.file.FileSpecOpStatus;
import com.perforce.p4java.core.file.IFileSpec;
import com.perforce.p4java.exception.AccessException;
import com.perforce.p4java.exception.P4JavaException;
import com.perforce.p4java.exception.ConnectionException;
import com.perforce.p4java.exception.RequestException;
import com.perforce.p4java.server.IServer;
import com.perforce.p4java.server.IServerInfo;
import com.perforce.p4java.server.ServerFactory;

/**
 *
 * @author Alan Snelson
 */
public class Perforce {

    private IServer server;
    private String hostname;
    private int port;
    private String username;
    private String password;
    private String root;

    public Perforce(String hostname, int port, String username, String password, String root) throws ConnectionException {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        connect();
    }

    private void connect() throws ConnectionException {
        try {
            server = ServerFactory.getServer("p4java://" + hostname + ":" + port, null);
            server.connect();
            server.setUserName(username);
            server.login(password);
        } catch (P4JavaException p4je) {
            throw new ConnectionException(p4je.getMessage());
        } catch (URISyntaxException urise) {
            throw new ConnectionException(urise.getMessage());
        }

    }

    public ArrayList<String> listDirectories(String path) {
        ArrayList<String> directories = new ArrayList();
        try {
            List<IFileSpec> dirList = server.getDirectories(
                    FileSpecBuilder.makeFileSpecList(new String[]{path + "/*"}), false, false, false);
            if (dirList != null) {
                for (IFileSpec dirSpec : dirList) {
                    if (dirSpec != null) {
                        String fullPath = dirSpec.getPreferredPathString();
                        int sep = fullPath.lastIndexOf("/");
                        String dirname = fullPath.substring(sep + 1);
                        directories.add(dirname);
                    }
                }
            }
        } catch (ConnectionException ce) {
            System.err.println(ce.getMessage());
        } catch (AccessException ae) {
            System.err.println(ae.getMessage());
        }
        return directories;
    }

    public List<IFileSpec> listFiles(String path) {
        List<IFileSpec> files = null;
        try {
            files = server.getDepotFiles(FileSpecBuilder.makeFileSpecList(new String[]{path + "/..."}), false);
        } catch (ConnectionException ce) {
            System.err.println(ce.getMessage());
        } catch (AccessException ae) {
            System.err.println(ae.getMessage());
        }
        return files;
    }

    public void play(String schema, MySQL db) {
        try {
            //Process DDL
            TreeMap<String, IFileSpec> scripts = new TreeMap();
            List<IFileSpec> fileList = listFiles(root + "/" + schema + "/ddl/...");
            for (IFileSpec fileSpec : fileList) {
                if (fileSpec != null) {
                    if (fileSpec.getOpStatus() == FileSpecOpStatus.VALID) {
                        String fullPath = fileSpec.getDepotPathString();
                        int dot = fullPath.lastIndexOf(".");
                        int sep = fullPath.lastIndexOf("/");
                        String filename = fullPath.substring(sep + 1, dot);
                        if (filename.startsWith("upgrade")) {
                            scripts.put(fullPath.substring(sep + 1, dot).replaceAll("[^\\d]", ""), fileSpec);
                        }
                    } else {
                        System.err.println(fileSpec.getStatusMessage());
                    }
                }
            }
            StringBuilder sb = new StringBuilder();
            String line;
            BufferedReader reader = new BufferedReader(new InputStreamReader(scripts.get(scripts.firstKey()).getContents(true), "UTF-8"));
            while ((line = reader.readLine()) != null) {
                if (!(line.startsWith("--") || line.startsWith("/*") || line.length() == 0)) {
                    sb.append(line);
                }
            }
            for (String sqlstmt : sb.toString().replaceAll("(?i)(^|;)(CREATE|DROP|INSERT|SET)", "$1ZZZZ$2").split("ZZZZ")) {
                if (!sqlstmt.equals("")) {
                    db.executeScript(schema, sqlstmt);
                }
            }
        } catch (SQLException sqle) {
        } catch (AccessException ae) {
        } catch (ConnectionException c) {
        } catch (RequestException re) {
        } catch (UnsupportedEncodingException uee) {
        } catch (IOException ioe) {
        }

    }
}
