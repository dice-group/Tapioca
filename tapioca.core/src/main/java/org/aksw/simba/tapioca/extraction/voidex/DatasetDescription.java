package org.aksw.simba.tapioca.extraction.voidex;

import java.util.ArrayList;
import java.util.List;

public class DatasetDescription {
    public String uri;
    public String title;
    public String description;
    public List<String[]> keyValuePairs = new ArrayList<String[]>();
    public List<DatasetDescription> subsets;
    public long triples;

    public DatasetDescription(String datasetUri) {
        this.uri = datasetUri;
    }

    public DatasetDescription(String uri, String title) {
        this.uri = uri;
        this.title = title;
    }

    public DatasetDescription(String uri, String title, String description) {
        this.uri = uri;
        this.title = title;
        this.description = description;
    }

    public void addSubset(DatasetDescription subset) {
        if (subsets == null) {
            subsets = new ArrayList<DatasetDescription>();
        }
        subsets.add(subset);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((uri == null) ? 0 : uri.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DatasetDescription other = (DatasetDescription) obj;
        if (uri == null) {
            if (other.uri != null)
                return false;
        } else if (!uri.equals(other.uri))
            return false;
        return true;
    }

}