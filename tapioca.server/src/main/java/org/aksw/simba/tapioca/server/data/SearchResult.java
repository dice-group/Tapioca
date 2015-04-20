package org.aksw.simba.tapioca.server.data;

import org.aksw.simba.tapioca.data.Dataset;

public class SearchResult {

    public Dataset dataset;
    public double similarity;

    public SearchResult(Dataset dataset, double similarity) {
        this.dataset = dataset;
        this.similarity = similarity;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public double getSimilarity() {
        return similarity;
    }

    public void setSimilarity(double similarity) {
        this.similarity = similarity;
    }
}
