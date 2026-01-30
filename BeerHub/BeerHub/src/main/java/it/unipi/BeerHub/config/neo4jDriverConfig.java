package it.unipi.BeerHub.config;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

public class neo4jDriverConfig {
    private static Driver graph_driver;
    private static final String URI = "bolt://10.1.1.54:7687";
    private static final String USERNAME = "neo4j";
    private static final String PASSWORD = "neo4jbeer";

    public static Driver getGraphDriver() {
        if(graph_driver == null){
            graph_driver = GraphDatabase.driver(URI, AuthTokens.basic(USERNAME, PASSWORD));
        }
        return graph_driver;
    }

    public static void closeGraphDriver() {
        if(graph_driver != null) graph_driver.close();
    }
}
