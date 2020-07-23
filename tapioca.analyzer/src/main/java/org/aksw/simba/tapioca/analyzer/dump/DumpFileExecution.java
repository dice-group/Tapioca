package org.aksw.simba.tapioca.analyzer.dump;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.aksw.simba.tapioca.analyzer.label.LabelExtractionUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumpFileExecution {

    private static final Logger LOGGER = LoggerFactory.getLogger(DumpFileExecution.class);

    private DumpFileAnalyzer voidCreator = new DumpFileAnalyzer();
    private DumpFileLabelExtractor labelExtractor = new DumpFileLabelExtractor();

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: DumpFileExecution input-directory output-directory label-directory");
            return;
        }
        File outputDir = new File(args[1]);
        outputDir.mkdir();
        File labelDir = new File(args[2]);
        labelDir.mkdir();
        DumpFileExecution exec = new DumpFileExecution();
        exec.analyze(args[0], outputDir, labelDir);
    }

    public void analyze(String fileName, File outputDir, File labelDir) {
        analyze(new File(fileName), outputDir, labelDir);
    }

    public void analyze(File file, File outputDir, File labelDir) {
        if (!file.exists()) {
            LOGGER.error("The given file \"" + file.getAbsolutePath() + "\" does not exist. Aborting.");
            return;
        }
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                analyze(f, outputDir, labelDir);
            }
        } else {
            // analyze
            LOGGER.info("Will process file {}", file);
            try {
                Model voidModel = createVoid(file.getAbsolutePath(),
                        new File(outputDir.getAbsolutePath() + File.separator + file.getName() + ".ttl"));
                createLabel(labelDir.getAbsolutePath() + File.separator + file.getName() + ".object",
                        file, voidModel);
            } catch (IOException e) {
                LOGGER.error("Could not read/write void model", e);
            }
            LOGGER.info("Proccessed file {}", file);
        }
    }

    public Model createVoid(String dumpFile, File outputFile) throws IOException {
        if (outputFile.exists()) {
            LOGGER.info("{} void already exists. Will use old one.", outputFile);
            // read file
            return ModelFactory.createDefaultModel().read(new FileReader(outputFile), "", "TTL");
        }
        Model voidModel = voidCreator.extractVoidInfo(dumpFile, dumpFile);
        try (FileWriter writer = new FileWriter(outputFile)) {
            voidModel.write(writer, "TTL");
        }
        return voidModel;
    }

    public File createLabel(String labelOutName, File dumpFile, Model voidModel) {
        String[][] labels = labelExtractor.extractLabels(LabelExtractionUtils.readUris(voidModel),
                dumpFile.getAbsolutePath());

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

        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(labelOutName))) {
            out.writeObject(labels);
        } catch (Exception e) {
            return null;
        }
        return new File(labelOutName);
    }

}
