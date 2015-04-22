package org.aksw.simba.tapioca.server;

public interface Engine {

    public static final String TAPIOCA_SIMILARITY_URI = "http://tapioca.aksw.org/vocabulary/similarity";

    public String retrieveSimilarDatasets(String voidString);
}
