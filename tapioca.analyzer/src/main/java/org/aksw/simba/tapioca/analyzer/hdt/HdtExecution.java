package org.aksw.simba.tapioca.analyzer.hdt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class HdtExecution {

	private static final Logger LOGGER = LoggerFactory.getLogger(HdtExecution.class);

	private HdtDumpFileAnalyzer voidCreator = new HdtDumpFileAnalyzer();
	private HdtLabelExtractor labelExtractor = new HdtLabelExtractor();

	
	private String prefix;
	
	public HdtExecution() {
		this("");
	}
	
	public HdtExecution(String prefix) {
		this.prefix = prefix;
		File f = new File("void");
		f.mkdir();
	}
	
	public static void main(String[] args) {
		String prefix="";
		if(args.length==0) {
			System.out.println("Usage: hdt-laundromat-exec laundromat.tsv [folder of hdt files]");
			return;
		}
		if(args.length>1) {
			prefix=args[1];
		}
		HdtExecution exec = new HdtExecution(prefix);
		exec.executeLaundromatTSV(args[0]);
	}
	
	public void executeLaundromatTSV(String fileName) {
		executeLaundromatTSV(new File(fileName));
	}
	
	public void executeLaundromatTSV(File file) {
		try(BufferedReader reader = new BufferedReader(new FileReader(file));PrintWriter pw = new PrintWriter("tmp_laundromat.tsv")){
			String line;
			//read header and ignore it
			pw.print(reader.readLine().replaceAll("\\s+", "\t")); 
			pw.println("\tvoid file\tlabels file");
			while((line=reader.readLine())!=null) {
				if(line.isEmpty()) {
					continue;
				}
				String[] row = line.split("\t");
				String hdtId = row[1];
				String namespace = row[0];
				Integer size = Integer.parseInt(row[3].replace("^^http://www.w3.org/2001/XMLSchema#integer", ""));
				if(size==0) {
					LOGGER.warn("Size of HDT file with ID {} is empty. Will ignore it.", hdtId);
					pw.println(line);
					continue;
				}
				LOGGER.info("Will process HDT file {} ({})", hdtId, size);
				try {
					File voidFile = createVoid(hdtId, namespace);
					Model voidModel = ModelFactory.createDefaultModel().read(new FileReader(voidFile), namespace, "TTL");
					File labels = createLabel(hdtId, new File(prefix+hdtId+"?type=hdt"),voidModel);
					//save into TSV
					pw.print(line);
					StringBuilder b = new StringBuilder("\t");
					b.append(voidFile.getAbsolutePath()).append("\t");
					if(labels!=null) {
						b.append(labels.getAbsolutePath());
					}
					pw.println(b.toString());
				}catch(IOException e) {
					LOGGER.error("Could not read/write void model", e);
				}
				LOGGER.info("\rProccessed HDT file {} ({})", hdtId, size);
			}
			file.delete();
			File newFile = new File("tmp_laundromat.tsv");
			newFile.renameTo(file);
		} catch (IOException e) {
			LOGGER.error("Could not read laundromat tsv file", e);
		}
	}
	
	public File createVoid(String id, String namespace) throws IOException {
		StringBuilder idBuilder = new StringBuilder(prefix).append(id)
				.append("?type=hdt");
		File f = new File("void/"+id+".ttl");
		if(f.exists()) {
			LOGGER.info("HDT {} void already exists. Will use old one.", id);
			//read file
			return f;
		}
		Model voidModel = voidCreator.extractVoidInfo(namespace, idBuilder.toString());
		
		voidModel.write(new FileWriter(f), "TTL");
		return f;
	}
	
	public File createLabel(String id, File hdtDumpFile, Model voidModel) {
		String outName = "void/"+id+"_labels.out";
		String[][] labels = labelExtractor.extractLabels(hdtDumpFile, voidModel);
		
//		try(PrintWriter pw = new PrintWriter(outName)){
//			for(String[] label : labels) {
//				StringBuilder b = new StringBuilder();
//				for(String  l : label) {
//					b.append(l).append("\t");
//				}
//				pw.println(b.toString());
//			}
//		}catch(IOException e) {
//			return null;
//		}
		
		try(FileOutputStream fileOut = new FileOutputStream(outName);
		         ObjectOutputStream out = new ObjectOutputStream(fileOut)){
		         out.writeObject(labels);
		}catch(Exception e) {
			return null;
		}
		return new File(outName);
	}
	
}
