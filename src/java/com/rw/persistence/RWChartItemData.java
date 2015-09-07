package com.rw.persistence;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.util.JSON;

public class RWChartItemData  {
	static final Logger log = Logger.getLogger(RWChartItemData.class.getName());

	private DBObject series_metadata = null;
	private mongoStore store = null;
	private mongoRepo repo = null;
	private String name = null;
	private RWObjectMgr objMgr = null;
	
	
	public RWChartItemData (String pname, RWObjectMgr context) throws Exception {
		  log.trace( "RWChartItemData : " + pname);
		  objMgr = context;
		  store = new mongoStore(objMgr.getTenantKey());
		  repo = new mongoRepo(objMgr.getTenantKey());
		  if ( pname != null ) {
			  name = new String(pname);
			  BasicDBObject query = new BasicDBObject();
			  query.put("name", name);
			  series_metadata = repo.getColl("ChartData").findOne(query);		
			  if ( series_metadata == null) {
				  System.out.println("Metadata missing for :" + pname);
			  }
		  }
	}
	
	public RWChartItemData (RWObjectMgr context) throws Exception {
		  objMgr = context;
		  store = new mongoStore(objMgr.getTenantKey());
		  repo = new mongoRepo(objMgr.getTenantKey());
	}
	
	// Default is a simple counter
	public BasicDBObject getGroupByCriteria(DBObject metadata, JSONObject context) throws Exception {
		
		BasicDBObject groupby_meta = (BasicDBObject) metadata.get("groupby");
		if( groupby_meta == null ) 	return null;
		
		BasicDBObject groupby = new BasicDBObject();
		BasicDBObject groupbyFields = null;
		
		BasicDBObject aggregate = (BasicDBObject) groupby_meta.get("aggregate");
		String unit = (((BasicDBObject)aggregate).containsField("unit"))?((BasicDBObject)aggregate).getString("unit"):null;
		Object fields = groupby_meta.get("field");
		if ( fields != null) {
			groupbyFields = new BasicDBObject();
			if (fields instanceof BasicDBList) {
				BasicDBList f = ( BasicDBList ) fields;
				for ( int i=0; i < f.size(); i++ ) {
					String fval = ((BasicDBObject)f.get(i)).getString("name");
					
					if (unit != null){groupbyFields.put("$"+unit, "$" + fval);}
					else{groupbyFields.put(fval, "$" + fval );}
					
				}
			}
			else  {
				String fval = ((BasicDBObject)fields).getString("name");
				if (unit != null){
					BasicDBObject y = new BasicDBObject();
					BasicDBObject u = new BasicDBObject();
					u.put("$"+unit, "$" + fval);
					y.put("$year","$" + fval);
					groupbyFields.put("year",y);
					groupbyFields.put(unit,u);
					//groupbyFields.put("$year", "$" + fval);
				}
				else{
					groupbyFields.put(fval, "$" + fval );
				}
				
			}
			//if (unit != null){
			//	groupby.put("date", groupbyFields );
			//}
			//else{
				groupby.put("_id", groupbyFields );
			//}
						
		}
		else {
			BasicDBObject expression = (BasicDBObject) groupby_meta.get("expression");
			
			if ( expression != null ) {
				String expr = expression.getString("expr");
				
				String contextVarName = expression.getString("contextVar");
				if ( contextVarName != null) {
					String [] contextVars = contextVarName.split(",");
					for ( int i = 0 ; i < contextVars.length; i++) {
						Object contextVarObj =  context.get(contextVars[i]);
						if ( contextVarObj != null )
							expr = expr.replace("%" + contextVars[i] + "%", contextVarObj.toString());
					}
				}
				groupbyFields = (BasicDBObject) JSON.parse(expr); 
				groupby.put("_id", groupbyFields );			
			}
			else { 				
				groupby.put("_id", aggregate.getString("name"));
			}
		}

		Object value = aggregate.get("value"); 
		BasicDBObject fn = new BasicDBObject(aggregate.getString("fn"), 
				value == null ? 1 : value);
		groupby.put(aggregate.getString("name"), fn);
		return groupby;
	}

	/*
	 * <expression code="[{$group : { _id : 'speakerRating' , speakerRating : { $avg : '$speakerRating'} }}]" ></expression>
	 * <groupby>
		    <field name="Speaker1_Id"></field>
		    <joinfield name='fullName' onKey="Speaker1_Id" toKey="_id" toCollection="rwParty"></joinfield>
			<aggregate name="average" fn="$avg"></aggregate>
		</groupby>
	 */
	
	// default is no projection, but a simple row counter
	public BasicDBObject getProjectionCriteria(DBObject metadata) {
		
		BasicDBObject project_meta = (BasicDBObject) metadata.get("project");
		if ( project_meta == null ) return null;

		BasicDBObject projection = new BasicDBObject();

		Object fields = project_meta.get("field");
		
		if ( fields != null) {
			if (fields instanceof BasicDBList) {
				BasicDBList f = ( BasicDBList ) fields;
				for ( int i=0; i < f.size(); i++ ) {
					String fval = ((BasicDBObject)f.get(i)).getString("name");
					projection.put(fval, 1);
				}
			}
			else  {
				String fval = ((BasicDBObject)fields).getString("name");
				projection.put(fval, 1);
			}	
			projection.put("_id",0);
		}
		return projection;		
	}


	// default is no projection, but a simple row counter
	public BasicDBObject getSortCriteria(DBObject metadata) {
		BasicDBObject sortby_meta = (BasicDBObject) metadata.get("sortby");
		BasicDBObject sortby = new BasicDBObject();
		if( sortby_meta == null ) {
			sortby.put("count", 1);
		}
		else  { sortby.put(sortby_meta.getString("fieldName"), sortby_meta.getString("order").equals("ASC") ? 1: -1); }
		return sortby;
		
	}
	
	public BasicDBObject getLimitCriteria(DBObject metadata) {
		BasicDBObject limit = null;
		if (metadata.containsField("limit")){
			limit = new BasicDBObject();
			Integer l =  (Integer)metadata.get("limit");
			limit.put("$limit", l);
		}
		
		return limit;
		
	}
	
	// <filter type ='time' fieldname="timestamp" period="6400" contextVar="context variable"></filter>
	public Map timeFilter(BasicDBObject filter, JSONObject context) throws Exception {
		String fieldname =filter.getString("fieldname");
		// period is in minutes, contextVar over rides the period param.
		Date compareToDate = new Date();
		String contextVarName = (filter.containsField("contextVar"))?filter.getString("contextVar"):null;
		if (contextVarName != null ) {
			Object dateStr = context.get(contextVarName);
			if ( dateStr instanceof Date)
				compareToDate = (Date) dateStr;
			else {			
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				compareToDate = dateFormat.parse(dateStr.toString());
			}
		}
		else {
			Long period = Long.parseLong(filter.getString("period"));
			if ( period != null ) {
				compareToDate = new Date(System.currentTimeMillis() - period * 60000);
				
			}
		}
		QueryBuilder qm = new QueryBuilder().start().put(fieldname).greaterThanEquals(compareToDate);
		System.out.println(qm.get().toMap());
		return qm.get().toMap();
	}
	
	// <filter type='expr' expression="{status : {'$not': 'IGNORE'}", contextVar="abc,xyz"></filter>	
	public Map exprFilter(BasicDBObject filter, JSONObject context) throws JSONException {
		String expr = filter.getString("expression");
		String contextVarName = filter.getString("contextVar");
		if ( contextVarName != null) {
			String [] contextVars = contextVarName.split(",");
			for ( int i = 0 ; i < contextVars.length; i++) {
				Object contextVarObj =  context.get(contextVars[i]);
				if ( contextVarObj != null )
					expr = expr.replace("%" + contextVars[i] + "%", contextVarObj.toString());
			}
		}
		BasicDBObject exprObj = (BasicDBObject) JSON.parse(expr); 
		Object[] exprKeys = exprObj.keySet().toArray();
		for (int i = 0; i < exprKeys.length; i++){
			String thisKey = (String)exprKeys[i];
			if (exprObj.get(thisKey) instanceof BasicDBObject){
				BasicDBObject exprTermVal = (BasicDBObject)exprObj.get(thisKey);
				Object[] exprTermValKeys = exprTermVal.keySet().toArray();
				for (int j = 0; j < exprTermValKeys.length; j++){
					String childKey = (String)exprTermVal.keySet().toArray()[j];
					if (childKey.equals("$lt") || childKey.equals("$lte") || childKey.equals("$gt") || childKey.equals("$gte")){
						String val = (String)exprTermVal.get(childKey);
						try {
							Date d = new SimpleDateFormat("EEE MMM d HH:mm:ss z yyyy",Locale.US).parse(val);
							exprTermVal.put(childKey, d);
							exprObj.put(thisKey,exprTermVal);
							// TODO Auto-generated catch block
							} catch (ParseException e2) {
								System.out.println("Date parse error -- attempted to parse expression = " + val);
							}
					}
				}
					
			}
		}
		return exprObj.toMap();
	}
	
	public Map InFilter(BasicDBObject filter, JSONObject context) throws JSONException {
		String fieldName = filter.getString("field");
		String contextVarName = filter.getString("contextVar");
		String sVal = context.getString(contextVarName);
		List<String> items = Arrays.asList(sVal.split("\\s*,\\s*"));
       	BasicDBList inList = new BasicDBList();
       	inList.addAll(items);
       	BasicDBObject inClause = new BasicDBObject();
       	inClause.put("$in", inList);
		BasicDBObject exprObj = new BasicDBObject();
		exprObj.put(fieldName, inClause);
		return exprObj.toMap();
	}

	// SPECFICICATION: <filter type='self' fieldname="fromId" attr="id" opr="$ne" contextVar="some context variable"></filter>
	public Map selfFilter(BasicDBObject filter, JSONObject context) throws Exception {
		log.trace( "selfQuerySpec");

		Object attrValue = null ;

		String contextVarName = filter.getString("contextVar");
		
		if (contextVarName != null ) {
			attrValue =  context.getString(contextVarName);
		}
		else {
			String attr =  filter.getString("attr");
			if  ( (attrValue = objMgr.GetSelfAttr(attr) ) == null ) 
			return null;
		}
		
		String fieldname =filter.getString("fieldname");
		
		QueryBuilder qm = null;
		if ( filter.get("opr") == null) {   // Equality
			qm = new QueryBuilder().start().put(fieldname).is((attrValue.toString()));
		}
		else {
			qm = new QueryBuilder().start().put(fieldname).is(new QueryBuilder().put(filter.getString("opr")).
					is(attrValue.toString()).get());
		}
		
		return qm.get().toMap();
	}
	
	
	public Map filter(BasicDBObject b, JSONObject context) throws Exception {
		String type = b.getString("ftype");
		if ( type.equals("time")) {
			return timeFilter(b, context);
		}
		else if ( type.equals("expr") ) {
			return exprFilter(b, context);
		}
		else if ( type.equals("self") ) {
			return selfFilter(b, context);
		}
		else if ( type.equals("IN") ) {
			return InFilter(b, context);
		}
		
		throw new Exception("filter Not found");
	}
	// default is no projection, but a simple row counter
	// SPECIFICATON: 
	
	// <filter type ='time' fieldname="timestamp" period="6400"></filter>
	// <filter type='expr' expression="{status : {'$not: 'IGNORE'}"></filter>
	// <filter type='self' fieldname="fromId" attr="id" opr="$ne"></filter>
	
	
	public BasicDBObject getMatchCriteria(DBObject metadata,JSONObject context) throws Exception {
		Object filter_meta = (Object) metadata.get("filter");
		Boolean noMetaFilter = false;
		BasicDBObject match = new BasicDBObject();
		if( filter_meta == null || filter_meta.equals("")) {
			noMetaFilter = true;
		} else {
		
			
			if (filter_meta instanceof BasicDBList) {
				BasicDBList a = (BasicDBList) filter_meta;
				for ( int i = 0; i < a.size(); i++) {
					BasicDBObject b = (BasicDBObject) a.get(i);
					match.putAll(filter(b, context));
				}
			}
			else  {
				BasicDBObject b = (BasicDBObject) filter_meta;
				match.putAll(filter(b,context));
			}
		}
		if (context != null){
			addContextFilters(context, match);
		} else {
			if (noMetaFilter){match = null;}
		}
		
		
		return match; 
	}
	/*
	public BasicDBObject getMatchCriteria2(JSONObject context) throws Exception {
		BasicDBObject match = new BasicDBObject();
		match.put("partytype", "PARTNER");
		
		return match; 
	}
	*/
	
	
	// project and count
	public BasicDBObject counter(String subject) {
		BasicDBObject group = new BasicDBObject();
		group.put("_id", "total");
		BasicDBObject counter = new BasicDBObject();
		counter.put("$sum", 1);
		group.put("total" ,counter);
		return group;
	}
	
	// ChartData can have a expression node with expr=code, contextVar
	// <expression  expr="mongo code" contextVar="v1,v2,v3"></expression>
	//
	public BasicDBList getExpression(DBObject metadata, JSONObject context) throws JSONException {
		BasicDBObject expr = (BasicDBObject) metadata.get("expression");;
		if ( expr == null ) return null;

		String code = expr.getString("code");		
		String contextVarName = expr.getString("contextVar");
		if ( contextVarName != null) {
			String [] contextVars = contextVarName.split(",");
			for ( int i = 0 ; i < contextVars.length; i++) {
				Object contextVarObj =  context.get(contextVars[i]);
				if ( contextVarObj != null )
					code = code.replace("%" + contextVars[i] + "%", contextVarObj.toString());
			}
		}
		
		BasicDBList exprObj = (BasicDBList) JSON.parse(code); 
		return exprObj;
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
	 *[ { "$match" : { "partytype" : "PARTNER"}} , { "$group" : { "_id" : { "OrgId" : "$OrgId"} , "OrgId" : { "$sum" : 1}}} , { "$sort" : { "count" : 1}}]
	 *
	 *
	 *  You can pipeline up to 10 stages
	 **/

	public BasicDBList getSeries(DBObject metadata, JSONObject context, BasicDBObject matchCriteria, BasicDBObject sortCriteria, BasicDBObject limitCriteria) throws Exception {
		log.trace( "getSeries");
		BasicDBList input = new BasicDBList();

		if ( matchCriteria == null ) matchCriteria = getMatchCriteria(metadata, context);
		DBObject match = (matchCriteria != null)?new BasicDBObject("$match", matchCriteria):null;
		if (match != null)	input.add(match);
		
		BasicDBObject projectCriteria = getProjectionCriteria(metadata);
		DBObject project = (projectCriteria != null)? new BasicDBObject("$project", projectCriteria ) : null;
		if ( project != null ) input.add(project);

		BasicDBObject groupCriteria = getGroupByCriteria(metadata, context);
		DBObject group = (groupCriteria != null)? new BasicDBObject("$group", getGroupByCriteria(metadata, context)) : null;
		if (group != null)	input.add(group);
		
		if ( sortCriteria == null ) sortCriteria = getSortCriteria(metadata);
		DBObject sort = (sortCriteria != null)? new BasicDBObject("$sort", sortCriteria):null;
		if (sort != null)	input.add(sort);
		
		if ( limitCriteria == null ) limitCriteria = getLimitCriteria(metadata);
		DBObject limit = (limitCriteria != null)? new BasicDBObject("$limit", limitCriteria):null;
		if (limit != null)	input.add(limit);
	    
		BasicDBList expr = getExpression(metadata, context);
		if (expr != null)	input.addAll(expr);
		System.out.println(input.toString());
		store.beginTrans();
		try {
			String coll = metadata.get("dataclass").toString();
			AggregationOutput output = null;
			int pipelineStage = input.size();
			switch(pipelineStage) {
			case 1:
				output = store.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0));
				break;
			case 2:
				output = store.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1));
				break;
			case 3:
				output = store.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2));
				break;
			case 4:
				output = store.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3)); 
				break;
			case 5:
				output = store.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3), 
						(BasicDBObject)input.get(4) );
				break;
			case 6:
				output = store.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3), 
						(BasicDBObject)input.get(4), (BasicDBObject)input.get(5) );
				break;
			case 7:
				output = store.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3), 
						(BasicDBObject)input.get(4), (BasicDBObject)input.get(5),
						(BasicDBObject)input.get(6));
				break;
			case 8:
				output = store.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3), 
						(BasicDBObject)input.get(4), (BasicDBObject)input.get(5),
						(BasicDBObject)input.get(6),(BasicDBObject)input.get(7));
				break;
			case 9:
				output = store.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3), 
						(BasicDBObject)input.get(4), (BasicDBObject)input.get(5),
						(BasicDBObject)input.get(6),(BasicDBObject)input.get(7),
						(BasicDBObject)input.get(8));
					break;
			case 10:
				output = store.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3), 
						(BasicDBObject)input.get(4), (BasicDBObject)input.get(5),
						(BasicDBObject)input.get(6),(BasicDBObject)input.get(7),
						(BasicDBObject)input.get(8),(BasicDBObject)input.get(9));
					break;
				
			}
			BasicDBList outputList = new BasicDBList();
			if ( output.getCommandResult().ok()) {
				for ( DBObject o : output.results() ) {
					if ( metadata.get("groupby") != null ) {
						Object joinfield = ((BasicDBObject) metadata.get("groupby")).get("joinfield");
						if( joinfield != null) {
							
							String[] joinfields = ((BasicDBObject) joinfield).getString("name").split(",");
							String onKey = ((BasicDBObject) joinfield).getString("onKey");
							String toKey = ((BasicDBObject) joinfield).getString("toKey");
							String toCollection = ((BasicDBObject) joinfield).getString("toCollection");
						
							BasicDBObject query = new BasicDBObject();
							
							BasicDBObject idObj = (BasicDBObject) o.get("_id");

							if (idObj == null || idObj.isEmpty() || idObj.getString(onKey).equals("")){
								for ( int i = 0; i < joinfields.length; i++) {
									o.put(joinfields[i], "No Value");
								}
							} else{
								query.put(toKey, new ObjectId(idObj.getString(onKey)));
								DBObject joinedData = store.getColl(toCollection).findOne(query);		
								for ( int i = 0; i < joinfields.length; i++) {
									if (joinfields[i] != null && joinedData != null && joinedData.get(joinfields[i]) != null){
										o.put(joinfields[i], joinedData.get(joinfields[i]));
									}
								}
							}
							
							
						}
					}
					outputList.add(o);
				}
			}
			return outputList;
		}
		catch(Exception e)
		{
			log.debug("API Error: ", e);
			throw e;
		}
		finally {
			store.endTrans();
		}
		
	}

	public BasicDBList getMultiSeries(JSONObject context) throws Exception {
		BasicDBList retVal = new BasicDBList();
		BasicDBList series = (BasicDBList) series_metadata.get("series");
		for ( int i = 0 ; i < series.size(); i++) {
			retVal.addAll(getSeries((BasicDBObject) series.get(i), context,null,null,null));
		}
		return retVal;
	}
	
	// short cut to a most basic chart, all driven thru meta data
	public BasicDBList getSeries(JSONObject context) throws Exception {
		log.trace( "getSeries :");
		if (!series_metadata.containsField("series")) 
			return getSeries(series_metadata, context, null, null,null);
		else {
			return getMultiSeries(context);
		}
	}
	
	// short cut to a most basic chart, all driven thru meta data
	public BasicDBList getSeries() throws Exception {
		log.trace( "getSeries :");
		if (!series_metadata.containsField("series")) 
			return getSeries(series_metadata, null, null, null,null);
		else return getMultiSeries(null);
	}

	// short cut to a most basic chart, all driven thru meta data
	/*
	public BasicDBList getSeries(JSONObject context, JSONObject chartDef) throws Exception {
		log.trace( "getSeries :");		
		metadata = (DBObject) JSON.parse(chartDef.toString());
		BasicDBObject sort = (metadata.containsField("sort"))?(BasicDBObject)metadata.get("sort"):null;
		return getSeries(context, null, sort);
		series_metadata = (DBObject) JSON.parse(chartDef.toString());
		if (!series_metadata.containsField("series")) 
			return getSeries(series_metadata, context, null, null);
		else return getMultiSeries(context);

	}*/
	public BasicDBList getSeries(JSONObject context, JSONObject chartDef) throws Exception {
		log.trace( "getSeries :");
		
		series_metadata = (DBObject) JSON.parse(chartDef.toString());
		BasicDBObject sort = (series_metadata.containsField("sort"))?(BasicDBObject)series_metadata.get("sort"):null;
		BasicDBObject limit = (series_metadata.containsField("limit"))?(BasicDBObject)series_metadata.get("limit"):null;
		context = (series_metadata.containsField("admin"))?null:context;
		if (!series_metadata.containsField("series")) 
			return getSeries(series_metadata, context, null, sort,limit);
		else return getMultiSeries(context);
	}

	
	public BasicDBList getMasterSeries(DBObject metadata,JSONObject context, BasicDBObject matchCriteria, BasicDBObject sortCriteria ) throws Exception {
		log.trace( "getMasterSeries");
		BasicDBList input = new BasicDBList();
		
		if ( matchCriteria == null ) matchCriteria = getMatchCriteria(metadata, context);
		DBObject match = (matchCriteria != null)?new BasicDBObject("$match", matchCriteria):null;
		if (match != null)	input.add(match);
		
		BasicDBObject projectCriteria = getProjectionCriteria(metadata);
		DBObject project = (projectCriteria != null)? new BasicDBObject("$project", projectCriteria ) : null;
		if ( project != null ) input.add(project);

		BasicDBObject groupCriteria = getGroupByCriteria(metadata,context);
		DBObject group = (groupCriteria != null)? new BasicDBObject("$group", getGroupByCriteria(metadata,context)) : null;
		if (group != null)	input.add(group);
		
		if ( sortCriteria == null ) sortCriteria = getSortCriteria(metadata);
		DBObject sort = (sortCriteria != null)? new BasicDBObject("$sort", sortCriteria):null;
		if (sort != null)	input.add(sort);
	    
		BasicDBList expr = getExpression(metadata,context);
		if (expr != null)	input.addAll(expr);
		System.out.println(input.toString());

		mongoMaster master = new mongoMaster();
		
		master.beginTrans();
		try {
			String coll = metadata.get("dataclass").toString();
			AggregationOutput output = null;
			int pipelineStage = input.size();
			
			
			switch(pipelineStage) {
			case 1:
				output = master.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0));
				break;
			case 2:
				output = master.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1));
				break;
			case 3:
				output = master.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2));
				break;
			case 4:
				output = master.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3)); 
				break;
			case 5:
				output = master.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3), 
						(BasicDBObject)input.get(4) );
				break;
			case 6:
				output = master.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3), 
						(BasicDBObject)input.get(4), (BasicDBObject)input.get(5) );
				break;
			case 7:
				output = master.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3), 
						(BasicDBObject)input.get(4), (BasicDBObject)input.get(5),
						(BasicDBObject)input.get(6));
				break;
			case 8:
				output = master.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3), 
						(BasicDBObject)input.get(4), (BasicDBObject)input.get(5),
						(BasicDBObject)input.get(6),(BasicDBObject)input.get(7));
				break;
			case 9:
				output = master.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3), 
						(BasicDBObject)input.get(4), (BasicDBObject)input.get(5),
						(BasicDBObject)input.get(6),(BasicDBObject)input.get(7),
						(BasicDBObject)input.get(8));
					break;
			case 10:
				output = master.getColl(coll).aggregate( 
						(BasicDBObject)input.get(0),(BasicDBObject)input.get(1),
						(BasicDBObject)input.get(2),(BasicDBObject)input.get(3), 
						(BasicDBObject)input.get(4), (BasicDBObject)input.get(5),
						(BasicDBObject)input.get(6),(BasicDBObject)input.get(7),
						(BasicDBObject)input.get(8),(BasicDBObject)input.get(9));
					break;
				
			}
			BasicDBList outputList = new BasicDBList();
			if ( output.getCommandResult().ok()) {
				for ( DBObject o : output.results() ) {
					if ( metadata.get("groupby") != null ) {
						Object joinfield = ((BasicDBObject) metadata.get("groupby")).get("joinfield");
						if( joinfield != null ) {
							
							String[] joinfields = ((BasicDBObject) joinfield).getString("name").split(",");
							String onKey = ((BasicDBObject) joinfield).getString("onKey");
							String toKey = ((BasicDBObject) joinfield).getString("toKey");
							String toCollection = ((BasicDBObject) joinfield).getString("toCollection");
							
							BasicDBObject query = new BasicDBObject();
							BasicDBObject idObj = (BasicDBObject) o.get("_id");
							
							query.put(toKey, new ObjectId(idObj.getString(onKey)));
							DBObject joinedData = master.getColl(toCollection).findOne(query);		
							for ( int i = 0; i < joinfields.length; i++) {
								o.put(joinfields[i], joinedData.get(joinfields[i]));
							}
						}
					}
					outputList.add(o);
				}
			}
			return outputList;
		}
		catch(Exception e)
		{
			log.debug("API Error: ", e);
			throw e;
		}		
		finally {
			master.endTrans();
		}
	}

	public BasicDBList getMultiMasterSeries(JSONObject context) throws Exception {
		BasicDBList retVal = new BasicDBList();
		BasicDBList series = (BasicDBList) series_metadata.get("series");
		for ( int i = 0 ; i < series.size(); i++) {
			retVal.addAll(getMasterSeries((BasicDBObject) series.get(i), context,null,null ));
		}
		return retVal;
	}
	
	public BasicDBList getMasterSeries(JSONObject context, BasicDBObject matchCriteria, BasicDBObject sortCriteria ) throws Exception {
		if (!series_metadata.containsField("series")) 
			return getMasterSeries(series_metadata, context, null, null);
		else return getMultiMasterSeries(context);
	}
	
	private BasicDBObject addContextFilters(JSONObject context, BasicDBObject match) throws Exception {
		JSONArray filters = new JSONArray();
		
		if (context.has("chart")){
			JSONObject chart = context.getJSONObject("chart");
			if (chart.has("filter")){
		
				if (chart.get("filter") instanceof JSONArray){
					filters = (JSONArray) chart.get("filter");
					for (int i = 0; i < filters.length(); i++){
						JSONObject fj = (JSONObject)filters.get(i);
						BasicDBObject fb = (BasicDBObject) com.mongodb.util.JSON.parse(fj.toString());
						match.putAll(filter(fb,context));
					}
				} else {
					if (chart.get("filter") instanceof JSONObject){
						JSONObject fj = (JSONObject)chart.get("filter");
						BasicDBObject fb = (BasicDBObject) com.mongodb.util.JSON.parse(fj.toString());
						match.putAll(filter(fb,context));
					}
				}
			}
						
			
		}
		return match; 
	}
	
	
}

