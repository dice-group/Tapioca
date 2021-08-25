# Tapioca
Linked data search engine

## How to generate a model (roughly)

1. Extract data
2. Create corpus
3. Build model

All commands are simplified and would have the form `java -cp <path-to-shaded-tapioca-jar-files> <complete-class-name> <arguments>`. 

### 1. Extract Data

First, the given set of datasets has to be analyzed to extract the data necessary for the model. To this end, `tapioca.analyzer` can be used. It comprises several classes but the main class that should be most helpful is `org.aksw.simba.tapioca.analyzer.dump.DumpFileExecution`. It should be called similar to

```
DumpFileExecution <input-directory> <output-directory> <label-directory>
```
It takes all dump files from the input directory, extracts (extended) VoID data and stores the data in the output directory before iterating a second time over the processed dump file to extract all labels. The latter are stored as serialized Java objects in the label directory.

### 2. Create Corpus

The initial corpus file is an XML file that is created by calling the the `org.aksw.simba.tapioca.gen.InitialCorpusCreation` class as follows:

```
InitialCorpusCreation <input-directory> <output-corpus-file>
```
The input directory should comprise all VoID files of the previous step.

The initial corpus simply comprises the VoID information in a different format. The `org.aksw.simba.tapioca.gen.LDACorpusCreation` class transforms this information to a corpus that can be used for the LDA algorithm. To this end, the label files and other sources of human readable representations of the URIs are used. In addition, this steps decides on the representation of the dataset. Since the paper showed that logarithmic counts of labels of properties led to the best results, this is the default configuration of the class.

```
LDACorpusCreation -n <input-corpus-file> -l <input-label-file> -o <output-corpus-file>
```
It should be noted that step 1 may have created several label files. The `-l` argument has to be repeated for each file. 

### 3. Build Model

Finally, the created corpus file is used to create a topic model using the `org.aksw.simba.tapioca.gen.ModelGenerator` class.

```
ModelGenerator -t <number-of-topics> -i 1040 -c <corpus-file> -o <output-directory>
```
The final model will be built and saved in the given output directory.

## How to use the model

There are two ways to use the model.

1. Start the Tapioca server
2. Include Tapioca into your application

### 2. Include tapioca

Include it as dependency in your pom file.

```xml
<dependency>
    <groupId>org.aksw.simba</groupId>
    <artifactId>tapioca.server</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```
Either build the project locally or include the following repository into your pom file.

```xml
<repositories>
    <repository>
        <id>maven.aksw.internal</id>
        <name>University Leipzig, AKSW Maven2 Repository</name>
        <url>https://maven.aksw.org/repository/internal</url>
    </repository>
    <repository>
        <id>maven.aksw.snapshots</id>
        <name>University Leipzig, AKSW Maven2 Repository</name>
        <url>https://maven.aksw.org/repository/snapshots</url>
    </repository>
</repositories>
```

Include the following code into your program

```Java
File labelFiles[] = {}; // TODO add label files
WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever = new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null, null, labelFiles, false);

File modelDirectoy; // TODO Set this file to the output directory of your model built step above
TMEngine engine = TMEngine.createEngine(cachingLabelRetriever, modelDirectoy, null, UriUsage.PROPERTIES, WordOccurence.LOG);

String voidString; // TODO This variable should contain the VoID metadata of the dataset for that link candidates should be retrieved.
engine.retrieveSimilarDatasets(voidString); // Returns a string with search results
```