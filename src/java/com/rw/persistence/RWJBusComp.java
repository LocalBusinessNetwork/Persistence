package com.rw.persistence;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONSerializers;
import com.mongodb.util.ObjectSerializer;

public class RWJBusComp {
	static final Logger log = Logger.getLogger(RWJBusComp.class.getName());
	
	private RWJBusObj bo;
	private DBObject metadata = null;
	private String name = null;
	private BasicDBObject emptyRecord = null;
	private int recordPointer = -1;

	public BasicDBObject currentRecord = null;
	public BasicDBList recordSet = null;
	private mongoStore store = null;
	private mongoRepo repo = null;
	private mongoMaster master = null;
	
	private List<String> ref_field_names = null; 
	private List<BasicDBObject> ref_fields = null; 
	private List<String> composite_field_names = null; 
	private List<BasicDBObject> composite_fields = null; 
	private List<String> date_field_names = null;
	private List<String> double_field_names = null;
	private List<String> upper_field_names = null;
	private String upperSuffix = "_upper";
	private boolean dirty_flag = false;
	
	public RWJBusComp (String pname, RWJBusObj bObj) throws Exception {
		  log.trace( "RWJBusComp : " + pname);
		  
		  bo = bObj;
		  name = new String(pname);
		  recordSet = new BasicDBList();
		  BasicDBObject query = new BasicDBObject();
		  query.put("name", name);
		  store = new mongoStore(bo.getApp().getContext().getTenantKey());
		  repo = new mongoRepo(bo.getApp().getContext().getTenantKey());
		  master = new mongoMaster();
		  metadata = repo.getColl("BusComp").findOne(query);
		  addSystemField("rw_created_on", "date");
		  addSystemField("rw_created_by", "reference");
		  addSystemField("rw_lastupdated_on", "date");
		  addSystemField("rw_lastupdated_by", "reference");
		  getEmptyRecord();
		  
		  
	}
	
	public boolean isDirty() {
		return dirty_flag;
	}
	
	public void addSystemField (String fieldName, String datatype) {
		log.trace( "addSystemField()");
		
		
		BasicDBObject field = new BasicDBObject();
		
		field.put("name", fieldName);
		field.put("type", "system");
		field.put("fldname", fieldName);
		field.put("datatype", datatype);
		field.put("readonly", "true");
		field.put("required", "true");

		if ( datatype.equals("reference") )
			field.put("reference", "rwParty");

		BasicDBList columns = (BasicDBList) metadata.get("field");
		columns.add(field);
		
	}
	
	public mongoStore getStore(){
		return store;
	}

	public mongoRepo getRepo(){
		return repo;
	}

	public mongoMaster getMaster(){
		return master;
	}

	public BasicDBObject getEmptyRecord() {
		log.trace( "getEmptyRecord()");

		if (emptyRecord == null ) {
			emptyRecord = new BasicDBObject();
			ref_fields = new ArrayList<BasicDBObject>();
			composite_fields = new ArrayList<BasicDBObject>();
			ref_field_names = new ArrayList<String>();
			composite_field_names = new ArrayList<String>();
			date_field_names = new ArrayList<String>();
			upper_field_names = new ArrayList<String>();
			double_field_names = new ArrayList<String>();
			
			BasicDBList columns = (BasicDBList) metadata.get("field");
			
			for (int i=0; i < columns.size(); i++) {
				BasicDBObject col = (BasicDBObject) columns.get(i);
				String fldname = col.get("fldname").toString();
				Object typeObj = col.get("type");
				String type = (typeObj == null) ? new String("system") : typeObj.toString();
				

				if (type.equals("reference")) {
					ref_field_names.add(fldname);
					ref_fields.add(col);
					emptyRecord.put(fldname, "");
				}else if (type.equals("composite")) {
					// composite fields are not saved to the db..
					composite_field_names.add(fldname);
					composite_fields.add(col);
				}
				else  {
					if (col.containsField("datatype") && (col.getString("datatype").equals("date") || col.getString("datatype").equals("datetime") || col.getString("datatype").equals("time"))) {
						// date fields need to be converted to proper format
						date_field_names.add(fldname);
					}	
					else if (col.containsField("datatype") && col.getString("datatype").equals("double")) {
						// date fields need to be converted to proper format
						double_field_names.add(fldname);
					}	
					// these are normal fields
					emptyRecord.put(fldname, "");
				}
				
				if (col.containsField("upperfield")){
					addSystemField(fldname+upperSuffix,"string");
					upper_field_names.add(fldname);
				}
			}
		}
		return emptyRecord;
	}
	public void NewRecord() {
		log.trace( "NewRecord()");

		currentRecord = (BasicDBObject) getEmptyRecord().clone();
		recordSet.add(currentRecord);
		dirty_flag = true;

	}
	
	public void pruneCurrentRecord() throws Exception {
		
		DBObject cl =  (DBObject) currentRecord.copy();
		Set<String> fs =  cl.keySet();
		
		for( String f : fs ) {
			Object v = cl.get(f);
			if ( (v == null) || v.toString().isEmpty()) {
				currentRecord.removeField(f);	
			}
		}
	}

	public void SaveRecordInner(database d) throws Exception {
		log.trace( "SaveRecordInner()");
		if ( dirty_flag == false ) return;
		pruneCurrentRecord();
		d.beginTrans();
		try {
			d.requestEnsureConnection();
			d.getColl(metadata.get("dataclass").toString()).save(currentRecord);
		}
		finally {
			d.endTrans();
		}
		AddCompositeFields(currentRecord);
		dirty_flag = false;
	}

	public void SaveRecord() throws Exception {
		log.trace( "SaveRecord()");
		SaveRecordInner(store);
	}

	public void SaveRecord(JSONObject data) throws Exception {
		log.trace( "SaveRecord()");

		log.trace( "SaveRecord - Data");
		BasicDBObject rec = getEmptyRecord();
		Set<String> keys= rec.keySet();
		for (String k:  keys ) {
			if ( !data.isNull(k) ) 
				SetFieldValue(k,data.get(k));
		}
		SaveRecord();
	}

	public void SaveRecordMaster() throws Exception {
		log.trace( "SaveRecordMaster()");
		SaveRecordInner(master);
	}

	public void SaveRecordMaster(JSONObject data) throws Exception {
		log.trace( "SaveRecordMaster()");

		log.trace( "SaveRecord - Data");
		BasicDBObject rec = getEmptyRecord();
		Set<String> keys= rec.keySet();
		for (String k:  keys ) {
			if ( !data.isNull(k) ) 
				SetFieldValue(k,data.get(k));
		}
		SaveRecordMaster();
	}

	public void SaveRecordRepo() throws Exception {
		log.trace( "SaveRecordRepo()");
		SaveRecordInner(repo);
	}

	public void SaveRecordRepo(JSONObject data) throws Exception {
		log.trace( "SaveRecord()");

		log.trace( "SaveRecord - Data");
		BasicDBObject rec = getEmptyRecord();
		Set<String> keys= rec.keySet();
		for (String k:  keys ) {
			if ( !data.isNull(k) ) 
				SetFieldValue(k,data.get(k));
		}
		SaveRecordMaster();
	}
	
	public void put(String fieldName,Object fieldValue) throws ParseException{
		log.trace( "put()");
		SetFieldValue(fieldName,fieldValue);
 	}
 	
 	public void SetFieldValue(String fieldName,Object fieldValue) throws ParseException{
		log.trace( "SetFieldValue()");
 		dirty_flag = true;

 		if ( date_field_names.contains(fieldName) && !(fieldValue instanceof Date) && fieldValue.equals("") == false)
 				if (fieldValue instanceof Long){
 					fieldValue = new Date ((Long) fieldValue);
 				} else {
 					if (fieldValue instanceof String){
 						fieldValue = new Date (Long.parseLong((String) fieldValue));
 					} 
 				}
 		else if ( double_field_names.contains(fieldName) && !(fieldValue instanceof Double && fieldValue.equals("") == false) )
 				if (fieldValue instanceof String && ((String) fieldValue).isEmpty() == false){
 					fieldValue = Double.parseDouble((String) fieldValue);
 				}
 		if (upper_field_names.contains(fieldName) && fieldValue != null){
 			String upperFieldVal = fieldValue.toString().toUpperCase();
 			currentRecord.put(fieldName+upperSuffix, upperFieldVal);
 		}
 		currentRecord.put(fieldName, fieldValue);
	}
	
 	public Object get(String fieldName){
		log.trace( "GetFieldValue()");
		return currentRecord.get(fieldName);
 	}
 	
	public Object GetFieldValue(String fieldName){
			log.trace( "GetFieldValue()");
			return currentRecord.get(fieldName);
			
	}
	
	public boolean has(String fieldName) {
		return currentRecord.containsField(fieldName);
	}
	
	public Object GetReferenceFieldValue(String fieldName) throws Exception{
		log.trace( "GetReferenceFieldValue : " + fieldName);
	
		if( ref_field_names.contains(fieldName) ) {
			Object id = currentRecord.get(fieldName);
			if ( id == null ) return null;

			String idStr = currentRecord.get(fieldName).toString();
			if ( idStr.isEmpty() || idStr == null ) return null;
			
			ObjectId idObj = new ObjectId(idStr);

			String joinTable = GetJoinTable(fieldName);
			if ( (joinTable == null) || joinTable.isEmpty())  return null;
			
			DBRef ref = new DBRef(store.getDB(), joinTable ,idObj);
			return ref.fetch();
		}
		
		return currentRecord.get(fieldName);
	}
	
	public String GetCompositeFieldValue(BasicDBObject metafield, BasicDBObject rec) {
		log.trace( "GetCompositeFieldValue : " + metafield.toString());

		String components = metafield.get("components").toString();
		String composer = metafield.get("composer").toString();
		String[] fields = components.split(",");
		
		switch (fields.length) {
		case 1:
			return 	String.format(composer, rec.getString(fields[0]).toString());
		case 2:
			return 	String.format(composer, rec.getString(fields[0]).toString(),
					rec.getString(fields[1]).toString());
		case 3:
			return 	String.format(composer, rec.getString(fields[0]).toString(),
					rec.getString(fields[1]).toString(),
					rec.getString(fields[2]).toString());
		case 4:
			return 	String.format(composer, rec.getString(fields[0]).toString(),
					rec.getString(fields[1]).toString(),
					rec.getString(fields[2]).toString(),
					rec.getString(fields[3]).toString());
		case 5:
			return 	String.format(composer, rec.getString(fields[0]).toString(),
					rec.getString(fields[1]).toString(),
					rec.getString(fields[2]).toString(),
					rec.getString(fields[3]).toString(),
					rec.getString(fields[4]).toString());
		case 6:
			return 	String.format(composer, rec.getString(fields[0]).toString(),
					rec.getString(fields[1]).toString(),
					rec.getString(fields[2]).toString(),
					rec.getString(fields[3]).toString(),
					rec.getString(fields[4]).toString(),
					rec.getString(fields[5]).toString());
		default:
			return null;
		}
	}
	
	public void AddCompositeFields(BasicDBObject rec) {
		log.trace( "AddCompositeFields()");

		for ( int i = 0; i < composite_fields.size(); i++) {
			String fldname = composite_fields.get(i).get("fldname").toString();
			rec.put(fldname, GetCompositeFieldValue(composite_fields.get(i), rec) );
		}
	}

	public void cloneRecord() {
		log.trace( "cloneRecord()");
		
		currentRecord = (BasicDBObject) currentRecord.copy();
		currentRecord.removeField("_id");
		dirty_flag = true;
		
	}
	
		public int UpsertInner(BasicDBObject filter, database d) throws Exception {
		log.trace( "UpsertInner" );		
		d.beginTrans();
		try {
			
		d.requestEnsureConnection();	
		DBCursor cursorDoc = null;
		
		cursorDoc = d.getColl(metadata.get("dataclass").toString()).find(filter);

		recordSet.clear();
		int recordCount = 0;
		while (cursorDoc.hasNext()) {
			BasicDBObject rec = (BasicDBObject) cursorDoc.next(); 
			recordSet.add(rec);
			recordCount++;
		}
		if ( recordCount > 0)
			setCurrentRecord(0);
		
		return recordCount;
		}
		finally {
			d.endTrans();
		}

	}

	public int UpsertQuery(BasicDBObject filter) throws Exception {
		log.trace( "UpsertQuery" );
		return UpsertInner(filter, store);
	}
	
	public int UpsertQueryRepo(BasicDBObject filter) throws Exception {
		log.trace( "UpsertQueryRepo" );
		return UpsertInner(filter, repo);
	}
	
	public int UpsertQueryMaster(BasicDBObject filter) throws Exception {
		log.trace( "UpsertQuery" );
		return UpsertInner(filter, master);
	}

	public int ExecInner(BasicDBObject filter, BasicDBObject sortSpec, int limit, int skip, database d) throws Exception {
		log.trace( "ExecInner" );
		
		if ( skip == -1 )
			return ExecInner(filter, sortSpec, limit, d);
		
		d.beginTrans();
		try {
		d.requestEnsureConnection();	
		DBCursor cursorDoc = null;
		
		if ( (sortSpec != null )  && !sortSpec.isEmpty()) {
			// sorting is optional
			cursorDoc = d.getColl(metadata.get("dataclass").toString()).find(filter, emptyRecord).sort(sortSpec).limit(limit).skip(skip);
		}
		else {
			cursorDoc = d.getColl(metadata.get("dataclass").toString()).find(filter,emptyRecord).limit(limit).skip(skip);
		}
		recordSet.clear();
		int recordCount = 0;
		while (cursorDoc.hasNext()) {
			BasicDBObject rec = (BasicDBObject) cursorDoc.next(); 
			AddCompositeFields(rec);
			recordSet.add(rec);
			recordCount++;
		}
		if ( recordCount > 0)
			setCurrentRecord(0);
		
		return recordCount;
		}
		finally {
			d.endTrans();
		}
		
	}
	
	public int ExecInner(BasicDBObject filter, BasicDBObject sortSpec, int limit, database d) throws Exception {
		log.trace( "ExecInner" );
		
		if ( limit == -1 )
			return ExecInner(filter, sortSpec, d);
		
		d.beginTrans();
		try {
		d.requestEnsureConnection();	
		DBCursor cursorDoc = null;
		
		if ( (sortSpec != null )  && !sortSpec.isEmpty()) {
			// sorting is optional
			cursorDoc = d.getColl(metadata.get("dataclass").toString()).find(filter,emptyRecord).sort(sortSpec).limit(limit);
		}
		else {
			cursorDoc = d.getColl(metadata.get("dataclass").toString()).find(filter,emptyRecord).limit(limit);
		}
		recordSet.clear();
		int recordCount = 0;
		while (cursorDoc.hasNext()) {
			BasicDBObject rec = (BasicDBObject) cursorDoc.next(); 
			AddCompositeFields(rec);
			recordSet.add(rec);
			recordCount++;
		}
		if ( recordCount > 0)
			setCurrentRecord(0);
		
		return recordCount;
		}
		finally {
			d.endTrans();
		}
		
	}

	public int ExecInner(BasicDBObject filter, BasicDBObject sortSpec, database d) throws Exception {
		log.trace( "ExecInner" );
		
		d.beginTrans();
		try {
		DBCursor cursorDoc = null;
		d.requestEnsureConnection();
		if ( (sortSpec != null )  && !sortSpec.isEmpty()) {
			// sorting is optional
			cursorDoc = d.getColl(metadata.get("dataclass").toString()).find(filter,emptyRecord).sort(sortSpec);
		}
		else {
			cursorDoc = d.getColl(metadata.get("dataclass").toString()).find(filter,emptyRecord);
		}
		recordSet.clear();
		int recordCount = 0;
		while (cursorDoc.hasNext()) {
			BasicDBObject rec = (BasicDBObject) cursorDoc.next(); 
			AddCompositeFields(rec);
			recordSet.add(rec);
			recordCount++;
		}
		if ( recordCount > 0)
			setCurrentRecord(0);
		
		return recordCount;
		}
		finally {
			d.endTrans();
		}
	}

	public int ExecQuery() throws Exception {
		return ExecInner(null,null, store);
	}
	
	public int ExecQuery(BasicDBObject filter) throws Exception {
		return ExecInner(filter,null, store);
	}

	public int ExecQuery(BasicDBObject filter, BasicDBObject sortSpec) throws Exception {
		log.trace( "ExecQuery" );
		return ExecInner(filter, sortSpec, store);
	}
	public int ExecQuery(BasicDBObject filter, BasicDBObject sortSpec, int limit) throws Exception {
		log.trace( "ExecQuery" );
		return ExecInner(filter,sortSpec, limit, store);
	}

	public int ExecQuery(BasicDBObject filter, BasicDBObject sortSpec, int limit, int skip) throws Exception {
		log.trace( "ExecQuery" );
		return ExecInner(filter,sortSpec, limit, skip, store);
	}



	public int ExecQueryRepo() throws Exception {
		log.trace( "ExecQueryRepo" );
		return ExecInner(null,null, repo);
	}

	public int ExecQueryRepo(BasicDBObject filter) throws Exception {
		return ExecInner(filter,null, repo);
	}

	public int ExecQueryRepo(BasicDBObject filter, BasicDBObject sortSpec) throws Exception {
		log.trace( "ExecQueryRepo" );
		return ExecInner(filter, sortSpec, repo);
	}

	public int ExecQueryRepo(BasicDBObject filter, BasicDBObject sortSpec, int limit) throws Exception {
		log.trace( "ExecQueryRepo" );
		return ExecInner(filter,sortSpec, limit, repo);
	}

	public int ExecQueryRepo(BasicDBObject filter, BasicDBObject sortSpec, int limit, int skip) throws Exception {
		log.trace( "ExecQueryRepo" );
		return ExecInner(filter,sortSpec, limit, skip, repo);
	}

	public int ExecQueryMaster() throws Exception {
		return ExecInner(null,null, master);
	}

	public int ExecQueryMaster(BasicDBObject filter) throws Exception {
		return ExecInner(filter,null, master);
	}

	public int ExecQueryMastrer(BasicDBObject filter, BasicDBObject sortSpec) throws Exception {
		log.trace( "ExecQueryMastrer" );
		return ExecInner(filter, sortSpec, master);
	}
	
	public int ExecQueryMaster(BasicDBObject filter, BasicDBObject sortSpec, int limit) throws Exception {
		log.trace( "ExecQueryMaster" );
		return ExecInner(filter,sortSpec, limit, master);
	}

	public int ExecQueryMaster(BasicDBObject filter, BasicDBObject sortSpec, int limit, int skip) throws Exception {
		log.trace( "ExecQueryMaster" );
		return ExecInner(filter,sortSpec, limit, skip, master);
	}
	
	public boolean setCurrentRecord(int index){
		log.trace( "setCurrentRecord" );
		
		int size = recordSet.size();
		if (  index < size  ) {
			recordPointer = index;
			currentRecord = (BasicDBObject) recordSet.get(recordPointer);
			dirty_flag = false;
			return true;
		}
		else return false;
	}

	private int getCurrentRecordIndex(){
			return recordPointer;
	}

	public Boolean FirstRecord(){
		if  (recordSet != null && recordSet.size() > 0){
			recordPointer = 0;
			setCurrentRecord(recordPointer);
			return true;
		} else{
			return false;
		}
	}
	
	public Boolean NextRecord(){
		if  (recordSet != null && recordPointer + 1 < recordSet.size()){
			recordPointer++;
			setCurrentRecord(recordPointer);
			return true;
		} else{
			return false;
		}		
	}
	
	public Boolean PreviousRecord(){
		if  (recordSet != null && recordPointer - 1 >= 0){
			recordPointer--;
			setCurrentRecord(recordPointer);
			return true;
		} else{
			return false;
		}		
	}
	
	public boolean DeleteRecordInner(database d) throws Exception {
		log.trace( "DeleteRecord" );
		String id = currentRecord.getString("_id");
		BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(id.toString()));
		String dataclass = metadata.get("dataclass").toString();
		//WriteResult wr = store.getColl(dataclass).remove(currentRecord);
		WriteResult wr = d.getColl(dataclass).remove(query);
		CommandResult cr = wr.getLastError();
		
		if (cr.ok() ) {
			recordSet.remove(currentRecord);
			// Move to Next Record by default, if at the end, move to previous record
			return NextRecord()?true:PreviousRecord();
		}
		else {
			log.debug(cr.getErrorMessage());
			return false;
		}
	}
	
	public boolean DeleteRecord() throws Exception {
		log.trace( "DeleteRecord" );
		return DeleteRecordInner(store);		
	}
	
	public boolean DeleteRecordRepo() throws Exception {
		log.trace( "DeleteRecordRepo" );
		return DeleteRecordInner(repo);		
	}

	public boolean DeleteRecordMaster() throws Exception {
		log.trace( "DeleteRecordMaster" );
		return DeleteRecordInner(master);		
	}

	private String GetJoinTable(String fieldName) {
		log.trace("GetJoinTable()");
		for (int i=0; i < ref_fields.size(); i++) {
			BasicDBObject col = (BasicDBObject) ref_fields.get(i);
			if (col.get("fldname").toString().equals(fieldName) )
				return col.get("entity").toString();
		}
		return null;

	}
	
	private boolean AddJoinedObject(BasicDBObject joinBC, BasicDBObject cRec) throws Exception{
		log.trace("AddJoinedObject()");
		
		  String joinRef = joinBC.getString("joinfield");
		  
		  BasicDBObject joinedRowData = (BasicDBObject) GetReferenceFieldValue(joinRef);
		  
		  if ( joinedRowData == null ) return false;

		  try {
			  BasicDBList joinColumns = (BasicDBList) joinBC.get("field"); 
			  for( int j = 0; j < joinColumns.size(); j++) {
				  BasicDBObject joinColumn = (BasicDBObject) joinColumns.get(j);
				  Object typeObj = joinColumn.get("type");
				  String type =( typeObj == null) ? new String("system") :typeObj.toString();
				  String fldname = joinColumn.get("fldname").toString();
				  if ( type.equals("composite") ) {
						cRec.put(fldname,GetCompositeFieldValue(joinColumn, joinedRowData));
				  }
				  else {
					  String srcFldName = joinColumn.get("src_fldname").toString();
					  cRec.put(fldname, joinedRowData.get(srcFldName));
				  }
			  }
		  } catch (ClassCastException e) {
					  // there was only a single join column
					  BasicDBObject joinColumn = (BasicDBObject) joinBC.get("field");
					  Object typeObj = joinColumn.get("type");
					  String type =( typeObj == null) ? new String("system") :typeObj.toString();
					  String fldname = joinColumn.get("fldname").toString();
					  if ( type.equals("composite") ) {
							cRec.put(fldname,GetCompositeFieldValue(joinColumn, joinedRowData));
					  }
					  else {
						  String srcFldName = joinColumn.get("src_fldname").toString();
						  cRec.put(fldname, joinedRowData.get(srcFldName));
					  }
		  }
		  return true;
	}
	
	// Composite records are needed rarely, so no need to suffer the performance for primary use cases.
	
	public BasicDBObject GetCompositeRecord() throws Exception {
		log.trace( "GetCompositeRecord" );
	
		BasicDBObject cRec = (BasicDBObject) currentRecord.clone();

		try {
			  // try of there  multiple join tables.
			  BasicDBList joins = (BasicDBList) metadata.get("join");
			  
			  if ( joins == null ) 
				  return cRec;
			  
			  for (int i= 0; i < joins.size(); i++) {
				  BasicDBObject joinBC = (BasicDBObject) joins.get(i);
				  if (joinBC != null )
					  try {
						  AddJoinedObject(joinBC, cRec);
					  }
					  catch (ClassCastException e) {
						  log.debug("API Error: ", e);
					  }
			  }
		}
		catch (ClassCastException e) {
			  // there was only a single join table
			  BasicDBObject join = (BasicDBObject) metadata.get("join");
			  AddJoinedObject(join, cRec);
		}
		return cRec;
	}

	public BasicDBList GetCompositeRecordList() throws Exception {
		log.trace( "GetCompositeRecordList" );
		BasicDBList cList = new BasicDBList();
		int index = getCurrentRecordIndex();
		if ( FirstRecord() ) {
			do {
				cList.add(GetCompositeRecord());		} 
			while (NextRecord());
			setCurrentRecord(index);
		}
		return cList;
	}

	public BasicDBObject GetObjectByID(String id) throws Exception{
		BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(id.toString())); 
        return (ExecInner(query,null, store) > 0 ? currentRecord:null);
	}
	
	public BasicDBObject GetObjectByIDRepo(String id) throws Exception{
		BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(id.toString())); 
        return (ExecInner(query,null, repo) > 0 ? currentRecord:null);
	}
	
	public BasicDBObject GetObjectByIDMaster(String id) throws Exception{
		BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(id.toString())); 
        return (ExecInner(query,null, master) > 0 ? currentRecord:null);
	}
	
	public BasicDBObject constructQuery(JSONObject data) throws JSONException{
		
		// TODO: redo this API using QueryBuilder
		
		BasicDBObject query = new BasicDBObject();
		
		BasicDBList columns = (BasicDBList) metadata.get("field");
		for (int i=0; i < columns.size(); i++) {
			BasicDBObject col = (BasicDBObject) columns.get(i);

			String fldname = col.get("fldname").toString();
			
			if (data.isNull(fldname) ) continue;
			
			Object value_in = data.get(fldname);
			
			if ( value_in != null ) {
				Object dataTypeObj = col.get("datatype");
				String datatype = (dataTypeObj == null) ? new String("string") : dataTypeObj.toString();
				if ( datatype.equals("string")) {
					query.put(fldname, new BasicDBObject("$regex", value_in.toString()));
				}
				// TODO: Add more data types like date, integer here
				else  {
					query.put(fldname,value_in.toString());
				}
			}
		}

		return query;
	}

	/*
	 * Documentation page:
	 * http://www.mongodb.org/display/DOCS/Using+The+Aggregation+Framework+with+The+Java+Driver
	 * http://docs.mongodb.org/manual/reference/aggregation/
	 * Examples : 
	 * 1) db.rwParty.aggregate({$match :{}}, {$project: {_id:1}},{$group : {_id:"$sum", count: { $sum: 1}}})
	 * return : { "result" : [ { "_id" : null, "count" : 11 } ], "ok" : 1 }
	 * 		number of records in this aggregate.
	 * 2) db.rwParty.aggregate({$match :{firstName:"Archana"}}, {$group : {_id:"$firstName", count: { $sum: 1}}})
	 * return  { "result" : [ { "_id" : "Archana", "count" : 2 } ], "ok" : 1 }
	 *		there are two contacts with first name = Archana.
	 *
	 **/

	public AggregationOutput ExecAggregate( BasicDBObject matchCriteria, BasicDBObject projectCriteria, BasicDBObject groupbyCriteria, BasicDBObject sortCriteria) throws Exception {
		log.trace( "ExecAggregate" );

		AggregationOutput output = null;
		
		DBObject project = (projectCriteria != null)?
			new BasicDBObject("$project", projectCriteria ) : null;
			
		DBObject match = (matchCriteria != null)?
			new BasicDBObject("$match", matchCriteria):null;

		DBObject sort = (sortCriteria != null)?
					new BasicDBObject("$sort", sortCriteria):null;
	
		DBObject group = new BasicDBObject("$group", groupbyCriteria);
		
		store.beginTrans();
		try {
			
			store.requestEnsureConnection();
			String coll = metadata.get("dataclass").toString();
			log.trace( "Db - fetch - Aggregate");

			if ( sort == null ) {
				output = ( project == null )?
					((match == null)? store.getColl(coll).aggregate(group):
						store.getColl(coll).aggregate( match, group)):
					(( match == null )?store.getColl(coll).aggregate( project, group):
						store.getColl(coll).aggregate( match, project, group));
			}
			else {
				output = ( project == null )?
						((match == null)? store.getColl(coll).aggregate(group, sort):
							store.getColl(coll).aggregate( match, group, sort)):
						(( match == null )?store.getColl(coll).aggregate( project, group,sort):
							store.getColl(coll).aggregate( match, project, group, sort));
			}
		}
		catch(Exception e)
		{
			log.debug("API Error: ", e);
		}
		finally {
			store.endTrans();
		}
		return output;
	}
	
	
	public BasicDBObject filter(String filter) {
		
		return (BasicDBObject) JSON.parse(filter);

	}
	
}
