package io.boodskap.iot.tools.export;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Options;

public class Main {
	
	private static final Options opts = new Options();
	
	static {
		opts.addRequiredOption("t", "target", true, "Target mode <export | import | curate> (export)");
		
		opts.addOption("n", "node", true, "Elasticsearch Node Name");
		opts.addOption("c", "cluster", true, "Elasticsearch Cluster Name");
		opts.addOption("h", "host", true, "Elasticsearch host (localhost)");
		opts.addOption("p", "port", true, "Elasticsearch transport port (9300)");
		opts.addOption("sp", "sport", true, "Elasticsearch search port (9200)");
		opts.addOption("d", "domain", false, "Comma separated domain keys (all)");
		opts.addOption("q", "query", true, "Filter documents by query string");
		opts.addOption("s", "size", true, "Fetch size (5000) for exporting, Batch size for importing (100)");
		opts.addOption("a", "alive", true, "Keepalive in millis (60000)");
		opts.addOption("v", "verbose", false, "Verbose mode (false)");
		opts.addOption("o", "out", true, "Output/Input directory (data)");
		opts.addOption("f", "format", true, "Exrt/Import Format <file|db> (db)");
		opts.addOption("i", "indexes", false, "Comma separated index names (all)");
		opts.addOption("r", "records", false, "Comma separated record ids (all)");
		opts.addOption("m", "messages", false, "Comma separated message ids (all)");
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {			
			@Override
			public void run() {
				Exporter.instance().close();
				Curator.instance().close();
				Importer.instance().close();
			}
		}));;
	}
	
	public Main() {
	}
	
	public static void main(String[] args) throws Exception {
		
		
		args = new String[] {
				"-t", "import",
				"-c", "bskp-es-cluster",
				"-n", "node1",
				"-d",
				"-f", "db",
				"-s", "2500"
		};
		
		
		CommandLineParser parser = new DefaultParser();
		CommandLine config = null;
		
		try {
			config = parser.parse(opts, args, false);
		}catch(MissingOptionException mex) {
			System.err.println(mex.getMessage());
			HelpFormatter hf = new  HelpFormatter();
			hf.printHelp("java -jar esdump.jar", opts);
			return;
		}
		
		String nodeName = config.getOptionValue("n");
		String clusterName = config.getOptionValue("c");
		String hostName =  config.getOptionValue("h", "localhost");
		int port = Integer.valueOf(config.getOptionValue("p", "9300"));
		int searchPort = Integer.valueOf(config.getOptionValue("sp", "9200"));
		String indexes = config.getOptionValue("i");
		String domainKeys = config.getOptionValue("d");
		String query = config.getOptionValue("q");
		int fetchSize = Integer.valueOf(config.getOptionValue("s", "5000"));
		int bulkSize = Integer.valueOf(config.getOptionValue("s", "100"));
		long keepAlive = Long.valueOf(config.getOptionValue("a", "60000"));
		boolean verbose = config.hasOption("v");
		String outFolder = config.getOptionValue("o", "data");
		String target =  config.getOptionValue("t", "export");
		String format = config.getOptionValue("f", "db");
		String records = config.getOptionValue("r");
		String messages = config.getOptionValue("m");
		
		switch(format) {
		case "file":
		case "db":
			break;
		default:
			System.err.format("Unknown format:%s, supported <file | db>\n", format);
			return;
		}
		
		if((null != records || null != messages) && null == domainKeys) {
			System.err.println("Domain is expected when records or messages needs to be exported");
			return;
		}
		
		switch(target) {
		case "export":
			
			if(!config.hasOption("i") && !config.hasOption("r") && !config.hasOption("m")) {
				HelpFormatter hf = new  HelpFormatter();
				hf.printHelp("java -jar esdump.jar", opts);
				Thread.sleep(200);
				System.err.println("One of these <indexes | records | messages> is required, you can combine these options");
				return;
			}
			
			if(null != domainKeys && null != query) {
				System.err.println("Both (domain and query) are not supported.");
				return;
			}
			
			Exporter exp = Exporter.instance();
			
			try {
				
				exp.setNodeName(nodeName);
				exp.setClusterName(clusterName);
				exp.setHost(hostName);
				exp.setPort(port);
				exp.setSearchPort(searchPort);
				exp.setQueryString(query);
				exp.setFetchSize(fetchSize);
				exp.setKeepAlive(keepAlive);
				exp.setDebug(verbose);
				exp.setOutFolder(outFolder);
				exp.setExportToDB(format.equals("db"));
				
				if(null != domainKeys) {
					String[] rvals = domainKeys.split(",");
					exp.setDomains(new HashSet<String>(Arrays.asList(rvals)));
				}else {
					exp.setExportAllDomains(config.hasOption("d"));
				}

				if(null != indexes) {
					String[] rvals = indexes.split(",");
					exp.setIndexes(new HashSet<String>(Arrays.asList(rvals)));
				}else {
					exp.setExportAllIndexes(config.hasOption("i"));
				}
				
				if(null != records) {
					String[] rvals = records.split(",");
					Set<Long> rids = new HashSet<Long>();
					for(String rid : rvals) {
						rids.add(Long.valueOf(rid));
					}
					exp.setRecords(rids);
				}else {
					exp.setExportAllRecords(config.hasOption("r"));
				}
				
				if(null != messages) {
					String[] mvals = messages.split(",");
					Set<Long> mids = new HashSet<Long>();
					for(String mid : mvals) {
						mids.add(Long.valueOf(mid));
					}
					exp.setMessages(mids);
				}else {
					exp.setExportAllMessages(config.hasOption("m"));
				}
				
				exp.setup();
				exp.start();
			}finally {
				exp.close();
			}
			break;
		case "import":
			Importer importer = Importer.instance();
			importer.setBulkSize(bulkSize);
			importer.setClusterName(clusterName);
			importer.setHost(hostName);
			importer.setPort(port);
			importer.setImportFromDB(format.equals("db"));
			importer.setNodeName(nodeName);
			importer.setOutFolder(outFolder);
			
			if(null != domainKeys) {
				String[] rvals = domainKeys.split(",");
				importer.setDomains(new HashSet<String>(Arrays.asList(rvals)));
			}else {
				importer.setImportAllDomains(config.hasOption("d"));
			}

			importer.setup();
			importer.start();
			break;
		case "curate":
			Curator curator = Curator.instance();
			curator.setDomainKey(domainKeys);
			curator.setOutFolder(outFolder);
			curator.curate();
			break;
		default:
			System.err.format("Unknown target:%s, supported <export | import | curate>\n", target);
			break;
		}
		
	}

}
