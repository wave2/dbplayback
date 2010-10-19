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
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.SortedMap;

import com.perforce.p4java.core.file.FileAction;
import com.perforce.p4java.core.file.FileSpecBuilder;
import com.perforce.p4java.core.file.IFileSpec;
import com.perforce.p4java.exception.AccessException;
import com.perforce.p4java.exception.P4JavaException;
import com.perforce.p4java.exception.ConnectionException;
import com.perforce.p4java.exception.RequestException;
import com.perforce.p4java.server.IServer;
import com.perforce.p4java.server.ServerFactory;

/**
 *
 * @author Alan Snelson
 */
public class Perforce implements Repository {

    private IServer server;
    private String hostname;
    private int port;
    private String username;
    private String password;
    private String root;
    private boolean verbose = true;

    public Perforce(String hostname, int port, String username, String password, String root) throws ConnectionException {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.root = root;
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

    private ArrayList<String> listDirectories(String path) {
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

    private List<IFileSpec> listFiles(String path) {
        List<IFileSpec> files = null;
        try {
            files = server.getDepotFiles(FileSpecBuilder.getValidFileSpecs(FileSpecBuilder.makeFileSpecList(new String[]{path})), false);
        } catch (ConnectionException ce) {
            System.err.println(ce.getMessage());
        } catch (AccessException ae) {
            System.err.println(ae.getMessage());
        }
        return files;
    }

    public String play(String schema, Database db) {
        String result = "";
        try {
            //Process DDL
            SortedMap<Integer, IFileSpec> scripts = new TreeMap();
            List<IFileSpec> fileList = listFiles(root + "/" + schema + "/ddl/*");
            for (IFileSpec fileSpec : fileList) {
                if (fileSpec != null) {
                    //May need to improve this and add other file actions to be ignored
                    if (fileSpec.getAction() != FileAction.DELETE) {
                        String fullPath = fileSpec.getDepotPathString();
                        int dot = fullPath.lastIndexOf(".");
                        int sep = fullPath.lastIndexOf("/");
                        String filename = fullPath.substring(sep + 1, dot);
                        if (filename.startsWith("upgrade")) {
                            scripts.put(new Integer(fullPath.substring(sep + 1, dot).replaceAll("[^\\d]", "")), fileSpec);
                        }
                    }
                }
            }

            //Check if schema exists and create if not
            if (db.checkSchema(schema)) {
                //Get last applied script
                int currentVersion = db.getVersion(schema);
                if (scripts.lastKey() > currentVersion) {

                    for (Map.Entry<Integer, IFileSpec> script : scripts.entrySet()) {
                        if (script.getKey() > currentVersion) {
                            if (verbose) {
                                System.out.println("Processing Script: " + script.getValue().getDepotPathString());
                            }
                            BufferedReader scriptContents = new BufferedReader(new InputStreamReader(script.getValue().getContents(true), "UTF-8"));
                            String errMessage = db.executeScript(schema, scriptContents);
                            //Update version table
                            if (errMessage.isEmpty()) {
                                if (db.setVersion(schema, script.getKey(), script.getValue().getDepotPathString(), 1, "")){
                                    //Success

                                }
                            } else {
                                result = errMessage;
                                if (db.setVersion(schema, script.getKey(), script.getValue().getDepotPathString(), 0, errMessage)){
                                    //Failure
                                 }
                            }
                        }
                    }
                }
            }
        } catch (AccessException ae) {
            System.err.println(ae.getMessage());
        } catch (ConnectionException ce) {
            System.err.println(ce.getMessage());
        } catch (RequestException re) {
            System.err.println(re.getMessage());
        } catch (UnsupportedEncodingException uee) {
            System.err.println(uee.getMessage());
        }

    return result;
    }
}
