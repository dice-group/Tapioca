# Tapioca
Linked data search engine

## How to use (roughly)

1. Extract data
2. Create corpus
3. Build model

### 1. Extract data

First, the given set of datasets has to be analyzed to extract the data necessary for the model. To this end, `tapioca.analyzer` can be used. It comprises several classes but the main class that should be most helpful is `org.aksw.simba.tapioca.analyzer.dump.DumpFileExecution`. It should be called similar to

```
DumpFileExecution <input-directory> <output-directory> <label-directory>
```
It takes all dump files from the input directory, extracts (extended) VoID data and stores the data in the output directory before iterating a second time over the processed dump file to extract all labels. The latter are stored as serialized Java objects in the label directory.

### 2. Create corpus

The initial corpus file is an XML file that is created by calling the the `org.aksw.simba.tapioca.gen.InitialCorpusCreation` class as follows:

```
InitialCorpusCreation <input-directory> <output-corpus-file>
```
The input directory should comprise all VoID files of the previous step.

The initial corpus simply comprises the VoID information in a different format. The `org.aksw.simba.tapioca.gen.LDACorpusCreation` class transforms this information to a corpus that can be used for the LDA algorithm. To this end, the label files and other sources of human readable representations of the URIs are used. In addition, this steps decides on the representation of the dataset. Since the paper showed that logarithmic counts of labels of classes and properties led to the best results, this is the default configuration of the class.

 