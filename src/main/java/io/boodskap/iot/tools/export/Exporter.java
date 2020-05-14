package io.boodskap.iot.tools.export;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class Exporter {
	
	private static final Exporter instance = new Exporter();
	
	private static final Set<String> BSKP_INDEXES = new HashSet<String>();
	
	private String host = "localhost";
	private int port = 9300;
	private int searchPort = 9200;
	private String clusterName;
	private String nodeName;
	private String queryString;
	private String outFolder = "data";
	private int fetchSize = 1000;
	private long keepAlive = 60000;
	private boolean debug = false;
	private boolean exportToDB = true;
	private boolean exportAllDomains = false;
	private boolean exportAllIndexes = false;
	private boolean exportAllRecords = false;
	private boolean exportAllMessages = false;
	private Set<String> indexes = new HashSet<String>();
	private Set<Long> records = new HashSet<Long>();
	private Set<Long> messages = new HashSet<Long>();
	private Set<String> domains = new HashSet<String>();
	
	
	private PreBuiltTransportClient client;
	private Connection connection = null;
	private PreparedStatement pstmt = null;
	
	
	private int totalDomains;
	private int currentDomain;
	private int totalIndexes;
	private int currentIndex;
	private int totalRIndexes;
	private int currentRIndex;
	private int totalMIndexes;
	private int currentMIndex;
	private long totalEIndexes = 0;
	private long totalERecords = 0;
	private long totalEMessages = 0;
	private long totalExported = 0;
	
	
	static {
		BSKP_INDEXES.add("bskp_access");;
		BSKP_INDEXES.add("bskp_alexa");
		BSKP_INDEXES.add("bskp_geofences");
		BSKP_INDEXES.add("bskp_locations");
		BSKP_INDEXES.add("bskp_locationhistory");
		BSKP_INDEXES.add("bskp_sms");
		BSKP_INDEXES.add("bskp_audits");
		BSKP_INDEXES.add("bskp_domains");
		BSKP_INDEXES.add("bskp_scripts");
		BSKP_INDEXES.add("bskp_scripts");
		BSKP_INDEXES.add("bskp_scriptjars");
		BSKP_INDEXES.add("bskp_scriptjars");
		BSKP_INDEXES.add("bskp_objects");
		BSKP_INDEXES.add("bskp_verticals");
		BSKP_INDEXES.add("bskp_verticalversions");
		BSKP_INDEXES.add("bskp_verticalspublished");
		BSKP_INDEXES.add("bskp_verticalsimported");
		BSKP_INDEXES.add("bskp_widgets");
		BSKP_INDEXES.add("bskp_widgetversions");
		BSKP_INDEXES.add("bskp_widgetspublished");
		BSKP_INDEXES.add("bskp_widgetsimported");
		BSKP_INDEXES.add("bskp_screens");
		BSKP_INDEXES.add("bskp_screenversions");
		BSKP_INDEXES.add("bskp_screensimported");
		BSKP_INDEXES.add("bskp_assets");
		BSKP_INDEXES.add("bskp_assetgroups");
		BSKP_INDEXES.add("bskp_devices");
		BSKP_INDEXES.add("bskp_devicegroups");
		BSKP_INDEXES.add("bskp_devicemodels");
		BSKP_INDEXES.add("bskp_domainassetgroups");
		BSKP_INDEXES.add("bskp_domaindevicegroups");
		BSKP_INDEXES.add("bskp_domainusergroups");
		BSKP_INDEXES.add("bskp_domainugmembers");
		BSKP_INDEXES.add("bskp_events");
		BSKP_INDEXES.add("bskp_eventregistrations");
		BSKP_INDEXES.add("bskp_fcms");
		BSKP_INDEXES.add("bskp_fcmdevices");
		BSKP_INDEXES.add("bskp_firmwares");
		BSKP_INDEXES.add("bskp_reporteddevices");
		BSKP_INDEXES.add("bskp_users");
		BSKP_INDEXES.add("bskp_userassetgroups");
		BSKP_INDEXES.add("bskp_userdevicegroups");
		BSKP_INDEXES.add("bskp_usergroups");
		BSKP_INDEXES.add("bskp_voices");
		BSKP_INDEXES.add("bskp_rulefailures");
		BSKP_INDEXES.add("bskp_sqltables");
		BSKP_INDEXES.add("bskp_sqltemplates");
		BSKP_INDEXES.add("bskp_cpools");
		BSKP_INDEXES.add("bskp_dbmetadatum");
		BSKP_INDEXES.add("bskp_dbtemplates");
		BSKP_INDEXES.add("bskp_plugins");
		BSKP_INDEXES.add("bskp_billing");
		BSKP_INDEXES.add("bskp_contact");
		BSKP_INDEXES.add("bskp_invoice");
		BSKP_INDEXES.add("bskp_billingtemplate");
		BSKP_INDEXES.add("bskp_notifications");
		BSKP_INDEXES.add("bskp_notificationspub");
		BSKP_INDEXES.add("bskp_outgoingcommands");
		BSKP_INDEXES.add("bskp_sentcommands");
		BSKP_INDEXES.add("bskp_files");
		BSKP_INDEXES.add("bskp_emails");
		BSKP_INDEXES.add("bskp_logs");
		
	}

	private Exporter() {
	}
	
	public static final Exporter instance() {
		return instance;
	}
	
	public void setup() throws ClassNotFoundException, SQLException, IOException {
		
		ensureOutput();
		
		if(exportToDB) {
			Class.forName("org.h2.Driver");
		}

		client= new PreBuiltTransportClient(
				  Settings.builder().put("client.transport.sniff", false)
				  					.put("node.name", nodeName)
				                    .put("cluster.name", clusterName).build()) ;
		
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
		
	}
	
	public void start(){
		
		try {
			
			boolean domainsListed = false;
			
			if(domains.isEmpty() && null == queryString) {
				System.out.println("Using match-all query");
			}else if(domains.isEmpty()) {
				System.out.println("Using custom query");
			}else{
				System.out.println("Using match-domain query");
			}
			
			if(exportAllDomains) {
				domains.addAll(listDomains());
			}

			if(exportAllIndexes) {
				indexes.addAll(BSKP_INDEXES);
			}
			
			if(domains.isEmpty() && (!records.isEmpty() || !messages.isEmpty() || exportAllRecords || exportAllMessages)) {
				domainsListed = true;
				domains.addAll(listDomains());
			}
			
			totalDomains = domains.size();
			
	        for(String domainKey : domains) {
	        	
	        	++currentDomain;
	        	totalIndexes = 0;
	        	currentIndex = 0;
	        	totalRIndexes = 0;
	        	currentRIndex = 0;
	        	totalMIndexes = 0;
	        	currentMIndex = 0;
	        	
				System.out.format("Exporting domain:%s [%d/%d]\n", domainKey, currentDomain, totalDomains);

	        	if(exportToDB) {
			        connection = DriverManager.getConnection(String.format("jdbc:h2:%s/%s;DB_CLOSE_ON_EXIT=FALSE", ensureOutput().getAbsolutePath(), domainKey), "sa", "" );
			        
			        Statement stmt = connection.createStatement();
			        
		        	stmt.executeUpdate( "CREATE TABLE IF NOT EXISTS EXPORTED (DKEY VARCHAR(16) NOT NULL, IDXNAME VARCHAR(256) NOT NULL, IDXTYPE VARCHAR(256), DOCID VARCHAR(256), DATA CLOB, PRIMARY KEY (DKEY, IDXNAME, IDXTYPE, DOCID));" );
			        
			        connection.commit();
			        stmt.close();
			        
			        connection.setAutoCommit(false);
			        pstmt = connection.prepareStatement("MERGE INTO EXPORTED VALUES (?,?,?,?,?)");
			        
	        	}
	        	
	        	if(!domainsListed) {
	        		
					totalIndexes = indexes.size();

					for(String index : indexes) {
						++currentIndex;
						export(connection, pstmt, domainKey, index);
					}	        		
	        	}
	        	
	        	{
					final Set<String> indexes = new HashSet<String>();
					
					if(exportAllRecords) {
						indexes.addAll(listIndexes(true, domainKey));
					}else {
						for(Long id : records) {
							String index = String.format("rec_%d_%s", id, domainKey);
							indexes.add(index);
						}
					}
					
					totalRIndexes = indexes.size();

					for(String index : indexes) {
						++currentRIndex;
						export(connection, pstmt, domainKey, index);
					}
	        		
	        	}
	        	
	        	{
					final Set<String> indexes = new HashSet<String>();
					
					if(exportAllMessages) {
						indexes.addAll(listIndexes(false, domainKey));
					}else {
						for(Long id : messages) {
							String index = String.format("msg_%d_%s", id, domainKey);						
							indexes.add(index);
						}
					}

					totalMIndexes = indexes.size();

					for(String index : indexes) {
						++currentMIndex;
						export(connection, pstmt, domainKey, index);
					}
	        		
	        	}
	        	
	        	if(exportToDB) {
	        		pstmt.close();
	        		connection.close();
	        	}
	        }
			
			
			System.out.format("Export complete, domains:%d, objects:%d, records:%d, messages:%d, total:%d\n", totalDomains, totalEIndexes, totalERecords, totalEMessages, totalExported);
			
		}catch(Exception ex) {
			ex.printStackTrace();
		}finally {
			close();
		}
		
	}
	
	protected void export(Connection connection, PreparedStatement pstmt, String domainKey, String index) {
		
		final long beginCount = totalExported;
		final String itype = index.substring(0, 4); 
		
		try {			
			
			switch(itype) {
			case "bskp":
				System.out.format("\tProcessing [%d/%d] index:%s\n", currentIndex, totalIndexes, index);
				break;
			case "rec_":
				System.out.format("\tProcessing Record [%d/%d] index:%s\n", currentRIndex, totalRIndexes, index);
				break;
			case "msg_":
				System.out.format("\tProcessing Message [%d/%d] index:%s\n", currentMIndex, totalMIndexes, index);
				break;
			}
			
			
			QueryBuilder query;
			
			if((index.startsWith("msg_") || index.startsWith("rec_") )|| ( null == domainKey && null == queryString)) {
				query = QueryBuilders.matchAllQuery();
			}else if(null == domainKey) {
				query = QueryBuilders.queryStringQuery(queryString);
			}else{
				query = QueryBuilders.matchQuery("domainKey", domainKey);
			}

			SearchRequest search = new SearchRequest(index);
			search.scroll(new TimeValue(keepAlive));
			SearchSourceBuilder builder = new SearchSourceBuilder();
			builder.query(query);
			builder.size(fetchSize);
			search.source(builder);
			
			SearchResponse response = client.search(search).actionGet();
			
			switch(itype) {
			case "bskp":
				System.out.format("\tExporting [%d/%d] index:%s, records: %d\n", currentIndex, totalIndexes, index, response.getHits().getTotalHits());
				break;
			case "rec_":
				System.out.format("\tExporting Record [%d/%d] index:%s, records: %d\n", currentRIndex, totalRIndexes, index, response.getHits().getTotalHits());
				break;
			case "msg_":
				System.out.format("\tExporting Message [%d/%d] index:%s, records: %d\n", currentMIndex, totalMIndexes, index, response.getHits().getTotalHits());
				break;
			}

			if(!process(connection, pstmt, domainKey, index, response)) {
				return;
			}
			
			
			while(null != response.getScrollId()) {
				
				response = client.searchScroll(new SearchScrollRequest(response.getScrollId()).scroll(new TimeValue(60000))).get();
				
				if(!process(connection, pstmt, domainKey, index, response)) {
					break;
				}
			}
			
			
		}catch(Exception ex) {
			ex.printStackTrace();
		}finally {
			final long endCount = totalExported;
			switch(itype) {
			case "bskp":
				System.out.format("\tExported [%d/%d] index:%s, records: %d\n", currentIndex, totalIndexes, index, (endCount - beginCount));
				break;
			case "rec_":
				System.out.format("\tExported Record [%d/%d] index:%s, records: %d\n", currentRIndex, totalRIndexes, index, (endCount - beginCount));
				break;
			case "msg_":
				System.out.format("\tExported Message [%d/%d] index:%s, records: %d\n", currentMIndex, totalMIndexes, index, (endCount - beginCount));
				break;
			}
		}
		
	}
	
	protected boolean process(Connection connection, PreparedStatement pstmt, String domainKey, String index, SearchResponse response) throws IOException, SQLException {
		
		
		final String itype = index.substring(0, 4); 
		SearchHit[] hits = response.getHits().getHits();
		
		if(null == hits || hits.length <= 0) return false;
		
		System.out.print(".");
		
		for(SearchHit hit : hits) {
			
			if(exportToDB) {
				
				int sidx = 1;
				
				//DKEY, IDXNAME, IDXTYPE, DOCID, DATA
				pstmt.setString(sidx++, domainKey);
				pstmt.setString(sidx++, hit.getIndex());
				pstmt.setString(sidx++, hit.getType());
				pstmt.setString(sidx++, hit.getId());
				pstmt.setCharacterStream(sidx++, new StringReader(hit.getSourceAsString()));
				pstmt.addBatch();
					
				
			}else {
				
				File indexFolder = ensureIndex(domainKey, index);
				File typeFolder = ensureType(indexFolder, hit.getType());
				File doc = new File(typeFolder, String.format("%s.json", hit.getId()));
				Files.write(Paths.get(doc.getAbsolutePath()), hit.getSourceAsString().getBytes());
				
			}
			
			++totalExported;
		
			switch(itype) {
			case "bskp":
				++totalEIndexes;
				break;
			case "rec_":
				++totalERecords;
				break;
			case "msg_":
				++totalEMessages;
				break;
			}
		}
		
		if(exportToDB) {
			pstmt.executeBatch();
			connection.commit();
		}
		
		return true;
	}
	
	protected File ensureOutput() throws IOException {
		
		File out = new File(outFolder);
		File file = new File(out, "elastic");
		file.mkdirs();
		
		if(!file.exists()) throw new IOException(String.format("Unable to create directory %s", file.getAbsolutePath()));
		
		return file;
	}
	
	protected File ensureIndex(String domainKey, String index) throws IOException {
		
		File out = ensureOutput();
		File ofile = new File(out, domainKey);
		File file = new File(ofile, index);
		file.mkdirs();
		
		if(!file.exists()) throw new IOException(String.format("Unable to create directory %s", file.getAbsolutePath()));
		
		return file;
	}
	
	protected File ensureType(File indexFolder, String type) throws IOException {
		
		File file = new File(indexFolder, type);
		file.mkdirs();
		
		if(!file.exists()) throw new IOException(String.format("Unable to create directory %s", file.getAbsolutePath()));
		
		return file;
	}
	
	protected Set<String> listDomains() throws IOException{
		
		Set<String> domainKeys = new HashSet<String>();
		
		SearchResponse response = client.prepareSearch("bskp_domains")
				   .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				   .setFetchSource(new String[]{"domainKey"}, null)
				   .setQuery(QueryBuilders.matchAllQuery())
				   .setSize(10000)
				   .execute()
				   .actionGet();

		for (SearchHit hit : response.getHits()){
			Map<String, Object> map = hit.getSource();
			domainKeys.add((String)map.get("domainKey"));
		}

		return domainKeys;
	}
	
	protected Set<String> listIndexes(boolean records, String domainKey) throws IOException{
		
		Set<String> indexes = new HashSet<String>();
		
		String urlString = String.format("http://%s:%d/_cat/indices/%s_*_%s", host, searchPort, records ? "rec": "msg", domainKey.toLowerCase());
		URL url = new URL(urlString);
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
		String line = null;
		
		while((line = reader.readLine()) != null) {
			String index = line.split(" ")[2];
			indexes.add(index);
		}
		
		reader.close();
		
		return indexes;
	}
	
	public void close() {
		try{if(null != client) client.close();}catch(Exception ex) {ex.printStackTrace();}
		try{if(null != pstmt && !pstmt.isClosed()) pstmt.close();}catch(Exception ex) {ex.printStackTrace();}
		try{if(null != connection && !connection.isClosed()) connection.close();}catch(Exception ex) {ex.printStackTrace();}
	} 

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getOutFolder() {
		return outFolder;
	}

	public void setOutFolder(String outFolder) {
		this.outFolder = outFolder;
	}

	public int getFetchSize() {
		return fetchSize;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	public long getKeepAlive() {
		return keepAlive;
	}

	public void setKeepAlive(long keepAlive) {
		this.keepAlive = keepAlive;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public boolean isExportToDB() {
		return exportToDB;
	}

	public void setExportToDB(boolean exportToDB) {
		this.exportToDB = exportToDB;
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public Set<Long> getRecords() {
		return records;
	}

	public void setRecords(Set<Long> records) {
		this.records = records;
	}

	public Set<Long> getMessages() {
		return messages;
	}

	public void setMessages(Set<Long> messages) {
		this.messages = messages;
	}

	public int getSearchPort() {
		return searchPort;
	}

	public void setSearchPort(int searchPort) {
		this.searchPort = searchPort;
	}

	public boolean isExportAllIndexes() {
		return exportAllIndexes;
	}

	public void setExportAllIndexes(boolean exportAllIndexes) {
		this.exportAllIndexes = exportAllIndexes;
	}

	public boolean isExportAllRecords() {
		return exportAllRecords;
	}

	public void setExportAllRecords(boolean exportAllRecords) {
		this.exportAllRecords = exportAllRecords;
	}

	public boolean isExportAllMessages() {
		return exportAllMessages;
	}

	public void setExportAllMessages(boolean exportAllMessages) {
		this.exportAllMessages = exportAllMessages;
	}

	public Set<String> getIndexes() {
		return indexes;
	}

	public void setIndexes(Set<String> indexes) {
		this.indexes = indexes;
	}

	public boolean isExportAllDomains() {
		return exportAllDomains;
	}

	public void setExportAllDomains(boolean exportAllDomains) {
		this.exportAllDomains = exportAllDomains;
	}

	public Set<String> getDomains() {
		return domains;
	}

	public void setDomains(Set<String> domains) {
		this.domains = domains;
	}

}
