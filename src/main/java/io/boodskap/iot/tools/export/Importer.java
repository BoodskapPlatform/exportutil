package io.boodskap.iot.tools.export;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class Importer {
	
	private static final Importer instance = new Importer();
	
	private String host = "localhost";
	private int port = 9300;
	private String clusterName;
	private String nodeName;
	private String outFolder = "data";
	private boolean importFromDB = true;
	private boolean importAllDomains = false;
	private int bulkSize = 100;
	private Set<String> domains = new HashSet<String>();
	private boolean debug = false;
	
	
	private PreBuiltTransportClient client;
	private Connection connection = null;
	private Statement pstmt = null;
	
	public static void main(String[] args) throws Exception {
		Importer imp = Importer.instance();
		imp.setClusterName("bskp-es-cluster");
		imp.setNodeName("node-1");
		imp.setImportAllDomains(true);
		imp.setImportFromDB(false);
		imp.setup();
		imp.start();
	}

	private Importer() {
	}
	
	public static final Importer instance() {
		return instance;
	}
	
	public void setup() throws ClassNotFoundException, SQLException, IOException {
		
		if(importFromDB) {
			Class.forName("org.h2.Driver");
		}

		client= new PreBuiltTransportClient(
				  Settings.builder().put("client.transport.sniff", false)
				  					.put("node.name", nodeName)
				                    .put("cluster.name", clusterName).build()) ;
		
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
		
	}
	
	
	public void start() {
		
		try {
			
			if(importFromDB) {
				importFromDB();
			}else {
				importFromFS();
			}
			
		}catch(Exception ex) {
			ex.printStackTrace();
		}finally {
			close();
		}
		
	}
	
	private void importFromDB() throws IOException, SQLException {
		
		Set<String> domainDBs =  listDomainsDBs();
		
		String lastIndexName = "";
		String lastIndexType = "";
		long imported = -1;
		long totalImported = 0;
		
		for(String domainDB : domainDBs) {
			
			long beginImported = totalImported;
			
			System.out.format("Importing domain DB %s\n", domainDB);
			
	        connection = DriverManager.getConnection(String.format("jdbc:h2:%s", domainDB), "sa", "" );
	        
	        if(debug) System.out.println("DB opened, opening statement...");
	        
	        pstmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	        
	        if(debug) System.out.println("Statement opened, queriying records...");
	        
	        ResultSet result = pstmt.executeQuery("SELECT * FROM EXPORTED");
	        List<IndexRequest> requests = new ArrayList<IndexRequest>();
	        
	        if(debug) System.out.println("Performing import...");
	        
	        while(result.next()) {
	        	
	        	String indexName = result.getString("IDXNAME");
	        	String indexType = result.getString("IDXTYPE");
	        	String docId = result.getString("DOCID");
	        	Reader docstream = result.getCharacterStream("DATA");
	        	byte[] data = IOUtils.toByteArray(docstream, Charset.defaultCharset());
	        	
	        	if(!lastIndexName.equals(indexName) || !lastIndexType.equals(indexType)) {
	        		
	        		if(imported != -1) {
	            		System.out.format("\n\tImported %s[%s] records:%d\n", lastIndexName, lastIndexType, imported);	        			
	        		}
	        		
	        		lastIndexName = indexName;
	        		lastIndexType = indexType;
	        		imported = 0;
	        		
	        		System.out.format("\tImporting %s[%s] ", indexName, indexType);
	        	}
	        	
				IndexRequest req = new IndexRequest(indexName, indexType, docId);
				req.source(data, XContentType.JSON);
				requests.add(req);
	        	
				if(result.isLast() || requests.size() >= bulkSize) {
					insert(requests);
					imported += requests.size();
					totalImported += requests.size();
					requests.clear();
					System.out.print(".");
				}
				
				if(result.isLast()) {
            		System.out.format("\n\tImported %s[%s] records:%d\n", lastIndexName, lastIndexType, imported);	        								
				}
	        }
	        
	        pstmt.close();
	        connection.close();
			
	        long endImported = totalImported;
    		System.out.format("\tImported %d records from domain %s\n", (endImported - beginImported), domainDB);
		}

		System.out.format("\nImport finished, total imported records:%d\n", totalImported);
	}
	
	private void importFromFS() throws IOException {
		
		long totalImported = 0;

		List<File> domainFolders = new ArrayList<File>();
		
		if(importAllDomains) {
			domainFolders.addAll(listDomainFiles());
		}else {
			
			for(String domain : domains) {
				domainFolders.add(ensureDomainFolder(domain));
			}
		}
		
		for(File domainFolder : domainFolders) {
			
			long beginImported = totalImported;
			
			System.out.format("Importing domain %s\n", domainFolder.getName());

			List<File> domainIndexes = listDomainIndexFiles(domainFolder);
			
			for(File indexFolder : domainIndexes) {
				
				List<File> indexTypeFolders =  listDomainIndexTypes(indexFolder);
				
				for(File indexTypeFolder : indexTypeFolders) {
					
					long imported = 0;
					
	        		System.out.format("\tImporting %s[%s] ", indexFolder.getName(), indexTypeFolder.getName());

	        		List<Tuple<String, File>> documents = listDomainIndexTypeDocuments(indexTypeFolder);
					
					while(!documents.isEmpty()) {
						
						int end = documents.size() > bulkSize ? bulkSize : documents.size();
						List<Tuple<String, File>> bdocs = documents.subList(0, end);
						List<IndexRequest> requests = new ArrayList<IndexRequest>();
						
						for(Tuple<String, File> document : bdocs) {
							byte[] data = Files.readAllBytes(Paths.get(document.v2().getAbsolutePath()));
							IndexRequest req = new IndexRequest(indexFolder.getName(), indexTypeFolder.getName(), document.v1());
							req.source(data, XContentType.JSON);
							requests.add(req);
						}
						
						insert(requests);
						imported += requests.size();
						totalImported += requests.size();
						
						bdocs.clear();
						
						System.out.print(".");
					}
					
            		System.out.format("\n\tImported %s[%s] records:%d\n", indexFolder.getName(), indexTypeFolder.getName(), imported);	        								
				}
			}
			
			long endImported = totalImported;
    		System.out.format("\tImported %d records from domain %s\n", (endImported - beginImported), domainFolder.getName());
		}
		
		System.out.format("\nImport finished, total imported records:%d\n", totalImported);
		
	}
	
	private void insert(List<IndexRequest> requests) {
		
		BulkRequestBuilder bulk = client.prepareBulk();
		
		requests.forEach(r -> {bulk.add(r);});
		
		BulkResponse res = bulk.get();
		
		if(res.hasFailures()) {
			throw new RuntimeException(res.buildFailureMessage());
		}
	}
	
	public void close() {
		try{if(null != client) client.close();}catch(Exception ex) {ex.printStackTrace();}
		try{if(null != pstmt && !pstmt.isClosed()) pstmt.close();}catch(Exception ex) {ex.printStackTrace();}
		try{if(null != connection && !connection.isClosed()) connection.close();}catch(Exception ex) {ex.printStackTrace();}
	}

	protected File outputFolder(){
		File out = new File(outFolder);
		return new File(out, "elastic");
	}
	
	protected File ensureDomainFolder(String domainKey) throws IOException {
		
		File root = outputFolder();
		File domain = new File(root, String.format("%s%s", domainKey, importFromDB ? ".mv.db": ""));
		
		if(!domain.exists()) throw new FileNotFoundException(domain.getAbsolutePath());

		if(importFromDB) {
			if(!domain.isFile()) throw new IOException(String.format("Invalid domain DB %s", domain.getAbsolutePath()));
		}else {
			if(!domain.isDirectory()) throw new IOException(String.format("Invalid domain folder %s", domain.getAbsolutePath()));
		}
		
		return domain;
	}
	
	protected List<File> listDomainFiles() throws IOException {
		
		List<File> domains = new ArrayList<>();
		
		File root = outputFolder();
		
		File folders[] = root.listFiles(new FileFilter() {
			@Override
			public boolean accept(File f) {
				if(importFromDB) {
					return f.isFile() && f.getName().endsWith(".mv.db");
				}else {
					return f.isDirectory();
				}
			}
		});
		
		if(null != folders && folders.length > 0) {
			domains.addAll(Arrays.asList(folders));
		}
		
		return domains;
	}
	
	protected Set<String> listDomainsDBs() throws IOException{
		
		Set<String> domainKeys = new HashSet<String>();

		List<File> domainFiles = listDomainFiles();
		
		for(File f : domainFiles) {
			
			String name = f.getAbsolutePath();
			
			name = name.substring(0, name.indexOf(".mv.db"));
			
			domainKeys.add(name);
		}

		return domainKeys;
	}

	protected List<File> listDomainIndexFiles(File domainFolder) throws IOException {
		
		List<File> indexes = new ArrayList<>();
		
		File folders[] = domainFolder.listFiles(new FileFilter() {
			@Override
			public boolean accept(File f) {
				return f.isDirectory();
			}
		});
		
		if(null != folders && folders.length > 0) {
			indexes.addAll(Arrays.asList(folders));
		}
		
		return indexes;
	}
	
	protected List<File> listDomainIndexTypes(File indexFolder) throws IOException {
		
		List<File> indexes = new ArrayList<>();
		
		File folders[] = indexFolder.listFiles(new FileFilter() {
			@Override
			public boolean accept(File f) {
				return f.isDirectory();
			}
		});
		
		if(null != folders && folders.length > 0) {
			indexes.addAll(Arrays.asList(folders));
		}
		
		return indexes;
	}
	
	protected List<Tuple<String, File>> listDomainIndexTypeDocuments(File indexTypeFolder){
		
		List<Tuple<String, File>> documents = new ArrayList<>();
		
		File files[] = indexTypeFolder.listFiles(new FileFilter() {
			@Override
			public boolean accept(File f) {
				return (f.isFile() && f.getName().endsWith(".json"));
			}
		});
		
		if(null != files && files.length > 0) {
			for(File f : files) {
				String id = f.getName().substring(0, f.getName().lastIndexOf("."));
				documents.add(new Tuple<>(id, f));
			}
		}
		
		return documents;
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

	public String getOutFolder() {
		return outFolder;
	}

	public void setOutFolder(String outFolder) {
		this.outFolder = outFolder;
	}

	public boolean isImporttAllDomains() {
		return importAllDomains;
	}

	public void setImportAllDomains(boolean importAllDomains) {
		this.importAllDomains = importAllDomains;
	}

	public Set<String> getDomains() {
		return domains;
	}

	public void setDomains(Set<String> domains) {
		this.domains = domains;
	}

	public int getBulkSize() {
		return bulkSize;
	}

	public void setBulkSize(int bulkSize) {
		this.bulkSize = bulkSize;
	}

	public boolean isImportFromDB() {
		return importFromDB;
	}

	public void setImportFromDB(boolean importFromDB) {
		this.importFromDB = importFromDB;
	}

	public boolean isImportAllDomains() {
		return importAllDomains;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

}
