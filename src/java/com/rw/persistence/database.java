package com.rw.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.ResourceBundle;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

public interface database {
	public DB getDB() throws Exception; 
	public void cleanUp() throws Exception;
	public DBCollection getColl(String col) throws Exception;
	public void beginTrans() throws Exception;
	public void endTrans() throws Exception;
	public void requestEnsureConnection() throws Exception;	
}
