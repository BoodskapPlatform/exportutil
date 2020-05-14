package io.boodskap.iot.tools.export;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

public class Curator {
	
	private static final Curator instance = new Curator();

	private String outFolder = "data";
	private String domainKey;
	
	private long curated = 0;
	private long written = 0;
	
	private Map<String, Map<String, CSVPrinter>> writers = new HashMap<>();
	private Map<String, Map<String, Set<String>>> domainFiles = new HashMap<>();
	
	private Curator() {
	}
	
	public static final Curator instance() {
		return instance;
	}
	
	public void curate() throws FileNotFoundException, IOException {
		
		try {
			
			File dir = new File(outFolder);
			File cass = new File(dir, "cassandra");
			File dump = new File(cass, "dump");
			File curated = new File(cass, "curated");
			
			curated.mkdirs();
			
			File[] csvFiles = dump.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".csv");
				}
			});
			
			List<String> files = new ArrayList<String>();
			
			for(File csvFile : csvFiles) {
				if(curate(curated, csvFile)) {
					files.add(csvFile.getName());
				}
			}
			
			PrintWriter allArchiveWriter = new PrintWriter(new File(curated, "archives.sh"));
			
			for(Entry<String, Map<String, Set<String>>> me : domainFiles.entrySet()) {
				
				if(me.getValue().isEmpty()) continue;
				
				String line = String.format("tar -czf %s-cassandra.tar.gz %s", me.getKey(), me.getKey());
				allArchiveWriter.println(line);
				
				PrintWriter domainArchiveWriter = new PrintWriter(new File(curated, String.format("%s-archive.sh", me.getKey())));
				domainArchiveWriter.println(line);
				domainArchiveWriter.flush();
				domainArchiveWriter.close();
				
				File domainDir = new File(curated, me.getKey());
				File importFile = new File(domainDir, "import.cql");
				PrintWriter writer = new PrintWriter(importFile);
				
				for(String file : me.getValue().keySet()) {
					
					Set<String> headers = me.getValue().get(file);
					Iterator<String> iter = headers.iterator();
					StringBuffer sb = new StringBuffer();
					
					while(iter.hasNext()) {
						sb.append(iter.next());
						sb.append(iter.hasNext() ? "," : "");
					}
					
					String table = file.substring(0, file.lastIndexOf("."));
					writer.println(String.format("COPY boodskapks.%s (%s) FROM './%s' WITH HEADER = TRUE;", table, sb, file));
					
				}
				
				writer.close();
			}
			
			allArchiveWriter.flush();
			allArchiveWriter.close();
			
		}finally {
			System.out.format("Curated %d domains, %d files, written %d total records\n", domainFiles.size(), curated, written);
		}
		
	}
	
	private boolean curate(File curated, File csvFile) throws FileNotFoundException, IOException {
		
		try {
			
			System.out.format("Curating %s\n", csvFile.getName());
			
			
			CSVParser parser = CSVFormat.RFC4180.withFirstRecordAsHeader().withQuote(null).parse(new FileReader(csvFile));
			
			final Integer keyPos = parser.getHeaderMap().get("domainkey");
			
			if(null == keyPos) {
				System.out.format("Skipping non domain file:%s\n", csvFile.getName());
				return false;
			}
			
			Iterator<CSVRecord> iter = parser.iterator();
			
			while(iter.hasNext()) {
				CSVRecord r = iter.next();
				String domainKey = r.get("domainkey");
				if(null != this.domainKey && !domainKey.equals(domainKey)) {
					continue;
				}
				write(domainKey, curated, csvFile, parser, r);
			}
			
			++this.curated;
			return true;
		}finally {
			closeWriters();
		}
		
	}
	
	private void write(String domainKey, File curated, File csvFile, CSVParser parser, CSVRecord record) throws IOException {
		
		Map<String, CSVPrinter> domainWriters = writers.get(domainKey);
		
		if(null == domainWriters) {
			domainWriters = new HashMap<String, CSVPrinter>();
			writers.put(domainKey, domainWriters);
		}
		
		CSVPrinter writer = domainWriters.get(csvFile.getName());
		
		if(null == writer) {
			
			File domainDir = new File(curated, domainKey);
			domainDir.mkdirs();
			File target = new File(domainDir, csvFile.getName());
			writer = new CSVPrinter(new FileWriter(target), CSVFormat.RFC4180.withQuote(null));
			writer.printRecord(parser.getHeaderMap().keySet());
			domainWriters.put(csvFile.getName(), writer);
			
		}
		
		Map<String, Set<String>> fileMap = domainFiles.get(domainKey);
		
		if(null == fileMap) {
			fileMap = new HashMap<>();
			domainFiles.put(domainKey, fileMap);
		}
		
		Set<String> headers = fileMap.get(csvFile.getName());
		
		if(null == headers) {
			headers = new LinkedHashSet<>(parser.getHeaderMap().keySet());
			fileMap.put(csvFile.getName(), headers);
		}
		
		writer.printRecord(record);
		writer.flush();
		++written;
	}
	
	private void closeWriters() {
		Collection<Map<String, CSVPrinter>> vals = writers.values();
		vals.forEach(m -> {
			m.values().forEach(w -> {
				try{w.close();}catch(Exception ex) {ex.printStackTrace();}
			});
		});
	}
	
	public void close() {
		closeWriters();
	}
	
	public String getOutFolder() {
		return outFolder;
	}

	public void setOutFolder(String outFolder) {
		this.outFolder = outFolder;
	}

	public String getDomainKey() {
		return domainKey;
	}

	public void setDomainKey(String domainKey) {
		this.domainKey = domainKey;
	}

	public static void main(String[] args) throws Exception {
		Curator c = new Curator();
		c.curate();
	}
}
