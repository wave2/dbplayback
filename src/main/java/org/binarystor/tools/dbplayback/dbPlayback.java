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

import org.binarystor.tools.dbplayback.config.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;

import com.perforce.p4java.exception.ConnectionException;

import org.yaml.snakeyaml.JavaBeanLoader;

import org.kohsuke.args4j.*;

import org.tmatesoft.svn.core.SVNErrorCode;
import org.tmatesoft.svn.core.SVNErrorMessage;
import org.tmatesoft.svn.core.SVNException;
import org.tmatesoft.svn.core.SVNNodeKind;
import org.tmatesoft.svn.core.SVNURL;
import org.tmatesoft.svn.core.io.ISVNEditor;
import org.tmatesoft.svn.core.internal.io.dav.DAVRepositoryFactory;
import org.tmatesoft.svn.core.internal.io.fs.FSRepositoryFactory;
import org.tmatesoft.svn.core.internal.io.svn.SVNRepositoryFactoryImpl;
import org.tmatesoft.svn.core.io.SVNRepository;
import org.tmatesoft.svn.core.io.SVNRepositoryFactory;
import org.tmatesoft.svn.core.io.diff.SVNDeltaGenerator;

import org.jivesoftware.smack.ChatManager;
import org.jivesoftware.smack.Chat;
import org.jivesoftware.smack.MessageListener;
import org.jivesoftware.smack.XMPPConnection;
import org.jivesoftware.smack.XMPPException;

/**
 *
 * @author Alan Snelson
 */
public class dbPlayback {
    //Command Line Arguments

    @Option(name = "--help")
    private boolean help;
    @Option(name = "-c", usage = "Path to dbPlayback Config File")
    private String dbPlaybackConfig;
    @Option(name = "-v")
    public static boolean verbose;
    // receives other command line parameters than options
    @Argument
    private List<String> arguments = new ArrayList<String>();
    private String notifyMessage = "";
    public static String version = "0.1";

    private void sendMail(Properties props, List<String> recipients, String subject, String message, String from) throws MessagingException {
        boolean debug = false;

        // create some properties and get the default Session
        javax.mail.Session session = javax.mail.Session.getDefaultInstance(props, null);
        session.setDebug(debug);

        // create a message
        javax.mail.Message msg = new MimeMessage(session);

        // set the from and to address
        InternetAddress addressFrom = new InternetAddress(from);
        msg.setFrom(addressFrom);

        InternetAddress[] addressTo = new InternetAddress[recipients.size()];

        int i = 0;
        for (String recipient : recipients) {
            addressTo[i] = new InternetAddress(recipient);
            i++;
        }
        msg.setRecipients(javax.mail.Message.RecipientType.TO, addressTo);


        // Optional : You can also set your custom headers in the Email if you Want
        msg.addHeader("MyHeaderName", "myHeaderValue");

        // Setting the Subject and Content Type
        msg.setSubject(subject);
        msg.setContent(message, "text/plain");

        if (verbose) {
            System.out.println("\n-- SMTP Notification --");
            System.out.println("Sending E-Mail using SMTP server " + props.get("mail.smtp.host"));
            System.out.println("Recipients:");
            for (String recipient : recipients) {
                System.out.println("    " + recipient);
            }
        }
        Transport.send(msg);
    }

    private void sendXMPP(String username, String password, String hostname, List<String> recipients, String text) {
        XMPPConnection connection = new XMPPConnection(hostname);
        try {
            connection.connect();
            connection.login(username, password);
            ChatManager chatmanager = connection.getChatManager();

            org.jivesoftware.smack.packet.Message message;

            for (String recipient : recipients) {
                Chat chat = chatmanager.createChat(recipient, new MessageListener() {

                    @Override
                    public void processMessage(Chat chat, org.jivesoftware.smack.packet.Message message) {
                        //System.out.println("Received message: " + message);
                    }
                });
                chat.sendMessage(text);
            }
            if (verbose) {
                System.out.println("\n-- XMPP Notification --");
                System.out.println("Sending XMPP message using XMPP server " + hostname);
                System.out.println("Recipients:");
                for (String recipient : recipients) {
                    System.out.println("    " + recipient);
                }
            }
        } catch (XMPPException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Main entry point for dbPlayback when run from command line
     *
     * @param  args  Command line arguments
     */
    public static void main(String[] args) {
        new dbPlayback().doMain(args);
    }

    /**
     * Parse command line arguments and invoke dbRecorder
     *
     * @param  args  Command line arguments
     */
    public void doMain(String[] args) {

        String usage = "Usage: java -jar dbPlayback.jar [-c Path to config.yml] [-v]\nOptions:\n    -c  Path to Config.yml\n    -v  Generate verbose output on standard output";
        CmdLineParser parser = new CmdLineParser(this);

        // if you have a wider console, you could increase the value;
        // here 80 is also the default
        parser.setUsageWidth(80);

        try {
            // parse the arguments.
            parser.parseArgument(args);

            if (help) {
                throw new CmdLineException("Print Help");
            }

            // after parsing arguments, you should check
            // if enough arguments are given.
            //if( arguments.isEmpty() )
            //throw new CmdLineException("No argument is given");

        } catch (CmdLineException e) {
            if (e.getMessage().equalsIgnoreCase("Print Help")) {
                System.err.println("dbPlayback.java Ver " + version + "\nThis software comes with ABSOLUTELY NO WARRANTY. This is free software,\nand you are welcome to modify and redistribute it under the BSD license" + "\n\n" + usage);
                return;
            }
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            // print usage.
            System.err.println(usage);
            return;
        }

        //Do we have a config file? if not try and open the default config.yml
        if (dbPlaybackConfig == null) {
            dbPlaybackConfig = "config.yml";
        }

        try {
            //Parse YAML config file
            JavaBeanLoader<dbPlaybackConfig> beanLoader = new JavaBeanLoader<dbPlaybackConfig>(dbPlaybackConfig.class);
            dbPlaybackConfig config = beanLoader.load(new FileReader(dbPlaybackConfig.replace('/', File.separatorChar)));
            if (!config.checkConfig()) {
                System.err.println(config.configError);
                System.exit(1);
            }

            //Create the Perforce Repository objects
            HashMap<String, Perforce> perforceRepositories = new HashMap<String, Perforce>();
            Repository[] repositories = config.getRepository();
            for (Repository repository : repositories) {
                try{
                    if (repository.getType().equals("perforce")){
                        perforceRepositories.put(repository.getName(), new Perforce(repository.getHostname(), repository.getPort(), repository.getUsername(), repository.getPassword(), repository.getRoot()));
                    }
                } catch (ConnectionException ce){
                    if (verbose){
                        System.out.println(ce.getMessage());
                    }
                    notifyMessage += "Failed to connect to Perforce repository: " + ce.getMessage();
                }
            }

            //Create the MySQL database objects
            HashMap<String, MySQL> mysqlDatabases = new HashMap<String, MySQL>();
            Database[] databases = config.getDatabase();
            for (Database database : databases){
                if (database.getType().equals("mysql")){
                    try{
                        mysqlDatabases.put(database.getName(), new MySQL(database.getHostname(), database.getPort(), database.getUsername(), database.getPassword()));
                    } catch (SQLException sqle){
                        if (verbose){
                            System.err.println(sqle.getMessage());
                        }
                    }
                    }
            }

            //Process Schemata
            Schema[] schemata = config.getSchemata();
            for (Schema schema : schemata){
                //What repo holds the scripts?
                if (perforceRepositories.containsKey(schema.getRepository())){
                    //What database are we using?
                    if (mysqlDatabases.containsKey(schema.getDatabase())){
                        perforceRepositories.get(schema.getRepository()).play(schema.getName(), mysqlDatabases.get(schema.getRepository()));
                    }
                }
            }

            //Notify if any changes found
            if (!notifyMessage.equals("")) {
                NotificationMethod[] notifications = config.getNotification();
                for (NotificationMethod notification : notifications) {
                    //SMTP Notification
                    if (notification.getMethod().equals("smtp")) {
                        //Set the host smtp address
                        Properties props = new Properties();
                        props.put("mail.smtp.host", notification.getServer());
                        List<String> recipients = new ArrayList<String>();
                        for (String recipient : notification.getRecipients()) {
                            recipients.add(recipient);
                        }
                        sendMail(props, recipients, "dbPlayback notification", notifyMessage, notification.getSender());
                    }
                    //XMPP Notification
                    if (notification.getMethod().equals("xmpp")) {
                        List<String> recipients = new ArrayList<String>();
                        for (String recipient : notification.getRecipients()) {
                            recipients.add(recipient);
                        }
                        sendXMPP(notification.getUsername(), notification.getPassword(), notification.getServer(), recipients, notifyMessage);
                    }
                }
            }

        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
            System.out.println("\n" + usage);
        } catch (MessagingException me) {
            System.out.println("Failed to send e-mail. Error: " + me.getMessage());
        }


    }
}
