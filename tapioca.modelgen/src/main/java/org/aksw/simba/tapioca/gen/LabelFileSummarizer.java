package org.aksw.simba.tapioca.gen;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.dice_research.topicmodeling.commons.io.StorageHelper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Summarizes the given label files by generating a single file containing URIs
 * and their labels.
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class LabelFileSummarizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(LabelFileSummarizer.class);

    private static final String LABEL_FILE_SUFFIX = "labels.out";

    private static final String TEMP_FILE = "temp-labels.csv";

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2) {
            LOGGER.error("Not enough arguments:\nLabelFileSummarizer <output-file> <input-directory>");
            // LOGGER.error("Not enough arguments:\nLabelFileSummarizer <output-file>
            // <input-files/dirs> ...");
        }
        LabelFileSummarizer summarizer = new LabelFileSummarizer();
        // for (int i = 1; i < args.length; ++i) {
        // summarizer.addFileOrDir(new File(args[i]));
        // }
        // summarizer.processAsStream(new File(args[1]));
        // summarizer.writeIntoSingleFile(new File(args[1]));
        summarizer.readTsv();
        summarizer.writeToFile(args[0]);
    }

    protected File tempFile = new File(TEMP_FILE);
    protected Map<String, ? super Collection<String>> labels = new HashMap<>();
    protected ExecutorService service = Executors.newFixedThreadPool(32);

    private void writeIntoSingleFile(File directory) {
        // try {
        // tempFile = File.createTempFile("labels", "");
        // } catch (IOException e) {
        // e.printStackTrace();
        // return;
        // }
        try (Writer tempWriter = new FileWriter(tempFile)) {
            for (File f : directory.listFiles()) {
                String[][] labels = (String[][]) StorageHelper.readFromFileSavely(f.getAbsolutePath());
                if ((labels != null) && (labels.length > 0)) {
                    for (int i = 0; i < labels.length; ++i) {
                        for (int j = 1; j < labels[i].length; ++j) {
                            tempWriter.write(labels[i][0]);
                            tempWriter.write('\t');
                            tempWriter.write(labels[i][j]);
                            tempWriter.write('\n');
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readTsv() {
        labels = new HashMap<>();
        try (Stream<String> stream = Files.lines(Paths.get(TEMP_FILE))) {
            stream.parallel().forEach(l -> addTsvLine(l));
            // stream.parallel().collect(Collectors.toConcurrentMap(LabelFileSummarizer::get1stTsvField,
            // LabelFileSummarizer::get2ndTsvField, (s1, s2) -> {
            // s1.addAll(s2);
            // return s1;
            // }));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void addTsvLine(String line) {
        String uri;
        String label;
        int pos = line.indexOf('\t');
        if (pos < 0) {
            return;
        }
        uri = line.substring(0, pos);
        label = line.substring(pos + 1);
        Set<String> labelsOfUri;
        synchronized (labels) {
            if (labels.containsKey(uri)) {
                labelsOfUri = (Set<String>) labels.get(uri);
            } else {
                labelsOfUri = Collections.synchronizedSet(new HashSet<String>());
                labels.put(new String(uri), (Collection<String>) labelsOfUri);
            }
        }
        labelsOfUri.add(new String(label));
    }

    protected static String get1stTsvField(String line) {
        int end = line.indexOf('\t');
        if (end < 0) {
            return line;
        } else {
            return line.substring(0, end);
        }
    }

    protected static List<String> get2ndTsvField(String line) {
        int start = line.indexOf('\t');
        if (start < 0) {
            return Collections.EMPTY_LIST;
        } else {
            List<String> result = new LinkedList<>();
            result.add(line.substring(start + 1));
            return result;
        }
    }

    public void addFileOrDir(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; ++i) {
                addFileOrDir(files[i]);
            }
        } else {
            service.execute(new Runnable() {
                public void run() {
                    addLabels(file);
                };
            });
        }
    }

    public void processAsStream(File directory) {
        labels = Arrays.stream(directory.listFiles()).parallel().filter(f -> f.getName().endsWith(LABEL_FILE_SUFFIX))
                .map(f -> (String[][]) StorageHelper.readFromFileSavely(f.getAbsolutePath()))
                .filter(a -> (a != null) && (a.length > 0)).flatMap(a -> createSets(a).stream())
                .collect(Collectors.toConcurrentMap(p -> p.getLeft(), p -> p.getRight(), (s1, s2) -> {
                    s1.addAll(s2);
                    return s1;
                }));
    }

    public List<Pair<String, Set<String>>> createSets(String uriToLabel[][]) {
        List<Pair<String, Set<String>>> sets = new ArrayList<>();
        Set<String> labelsOfUri;
        for (int i = 0; i < uriToLabel.length; ++i) {
            labelsOfUri = new HashSet<String>();
            for (int j = 1; j < uriToLabel[i].length; ++j) {
                labelsOfUri.add(uriToLabel[i][j]);
            }
            sets.add(new ImmutablePair<String, Set<String>>(uriToLabel[i][0], labelsOfUri));
        }
        return sets;
    }

    public void addLabels(File file) {
        String uriToLabel[][] = StorageHelper.readFromFileSavely(file.getAbsolutePath());
        if (uriToLabel == null) {
            LOGGER.error("Couldn't load labels from \"" + file + "\". Returning null.");
            return;
        }
        Set<String> labelsOfUri;
        for (int i = 0; i < uriToLabel.length; ++i) {
            synchronized (labels) {
                if (labels.containsKey(uriToLabel[i][0])) {
                    labelsOfUri = (Set<String>) labels.get(uriToLabel[i][0]);
                } else {
                    labelsOfUri = Collections.synchronizedSet(new HashSet<String>());
                    labels.put(uriToLabel[i][0], (Collection<String>) labelsOfUri);
                }
            }
            for (int j = 1; j < uriToLabel[i].length; ++j) {
                labelsOfUri.add(uriToLabel[i][j]);
            }
        }
    }

    /**
     * Generates an array containing the information of the given labels map.
     * <p>
     * The method creates an array with the size <code>labels.size()</code>
     * containing a String array for every URI. This array starts with the URI
     * itself at position <code>0</code>. The other positions (&gt;=1) contain the
     * single tokens of the labels of this URI.
     * </p>
     * 
     * @param labels
     *            a mapping of URIs and a Set containing their labels
     * @return the generated String array
     */
    public static String[][] generateArray(Map<String, Collection<String>> labels) {
        String uriToLabel[][] = new String[labels.size()][];
        int pos = 0, pos2;
        String tokenizedLabel[];
        for (Entry<String, Collection<String>> labelsOfUri : labels.entrySet()) {
            tokenizedLabel = new String[labelsOfUri.getValue().size() + 1];
            pos2 = 0;
            tokenizedLabel[0] = labelsOfUri.getKey();
            for (String label : labelsOfUri.getValue()) {
                ++pos2;
                tokenizedLabel[pos2] = label;
            }
            uriToLabel[pos] = tokenizedLabel;
            ++pos;
        }
        return uriToLabel;
    }

    public void writeToFile(String file) throws InterruptedException {
        service.shutdown();
        service.awaitTermination(10, TimeUnit.DAYS);
        StorageHelper.storeToFileSavely(generateArray((Map<String, Collection<String>>) labels), file);
    }
}
