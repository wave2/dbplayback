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

package org.binarystor.tools.dbplayback.config;

/**
 *
 * @author Alan Snelson
 */
public class dbPlaybackConfig {
    private Repository[] repository;
    private Database[] database;
    private NotificationMethod[] notification;
    private Schema[] schemata;
    public String configError = "";

    public NotificationMethod getNotification(int index) {
        return this.notification[index];
    }

    public NotificationMethod[] getNotification() {
        return this.notification;
    }

    public void setNotification(int index, NotificationMethod value) {
        this.notification[index] = value;
    }

    public void setNotification(NotificationMethod[] value) {
        this.notification = value;
    }

    public Database getDatabase(int index) {
        return this.database[index];
    }

    public Database[] getDatabase() {
        return this.database;
    }

    public void setDatabase(int index, Database value) {
        this.database[index] = value;
    }

    public void setDatabase(Database[] values) {
        this.database = values;
    }

    public Repository getRepositorys(int index) {
        return this.repository[index];
    }

    public Repository[] getRepository() {
        return this.repository;
    }

    public void setRepository(int index, Repository value) {
        this.repository[index] = value;
    }

    public void setRepository(Repository[] value) {
        this.repository = value;
    }

    public Schema getSchemata(int index) {
        return this.schemata[index];
    }

    public Schema[] getSchemata() {
        return this.schemata;
    }

    public void setSchemata(int index, Schema value) {
        this.schemata[index] = value;
    }

    public void setSchemata(Schema[] value) {
        this.schemata = value;
    }

    public boolean checkConfig(){
        //Check Repositories
        if (this.repository == null){
            configError = "No Repositories found - please check config file.";
            return false;
        } else {
                for (Repository repo: this.repository){
                    if (repo.getName() == null){
                        configError = "Repository name required. e.g. - name: MyRepo";
                        return false;
                    }
                    if (repo.getHostname() == null){
                        configError = "Repository hostname required. e.g. hostname: perforce.mydomain.com";
                        return false;
                    }
                }
        }
        //Check Notification methods
        if (this.notification == null){
            configError = "No notification methods found - please check config file.";
            return false;
        } else {
            for (NotificationMethod notify: this.notification){
                if (notify.getMethod() == null){
                    configError = "Notification method required. e.g. - method: smtp";
                    return false;
                }else{
                    if (notify.getRecipients() == null){
                        configError = "Notification recipients required. e.g. recipients: [me@mycompany.com]";
                        return false;
                    }
                }
            }
        }
        //Check MySQL instances
        if (this.database != null){

        }
        return true;
    }

}
