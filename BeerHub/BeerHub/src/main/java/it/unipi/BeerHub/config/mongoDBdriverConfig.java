package it.unipi.BeerHub.config;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class mongoDBdriverConfig {
    private static MongoClient client;
    private static MongoDatabase database;
    private static MongoCollection<Document> beers;
    private static MongoCollection<Document> breweries;
    private static MongoCollection<Document> reviews;
    private static MongoCollection<Document> users;
    private static final String URI = "mongodb://10.1.1.54:27017,10.1.1.58:27017,10.1.1.60:27017/?replicaSet=lsmdb";;
    private static final String DATABASE = "BeerHub";

    private static final MongoClientSettings settings = MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(URI))
            .writeConcern(WriteConcern.W1.withJournal(true))
            .readConcern(ReadConcern.LOCAL)
            .readPreference(ReadPreference.nearest())
            .build();

    public static MongoClient getMongoClient() {
        if(client == null){
            client = MongoClients.create(settings);
        }
        return client;
    }

    public static MongoDatabase getDatabase() {
        if(client == null){
            client = MongoClients.create(settings);
        }
        if(database == null){
            database = client.getDatabase(DATABASE);
        }
        return database;
    }

    public static MongoCollection<Document> getCollectionIst(String collectionName) {
        if(client == null){
            client = MongoClients.create(settings);
        }
        if(database == null){
            database = client.getDatabase(DATABASE);
        }

        return switch (collectionName) {
            case "beers" -> {
                if (beers == null) {
                    beers = database.getCollection(collectionName, Document.class);
                }
                yield beers;
            } case "breweries" -> {
                if (breweries == null) {
                    breweries = database.getCollection(collectionName, Document.class);
                }
                yield breweries;
            } case "users" -> {
                if (users == null) {
                    users = database.getCollection(collectionName, Document.class);
                }
                yield users;
            } case "reviews" -> {
                if (reviews == null) {
                    reviews = database.getCollection(collectionName, Document.class);
                }
                yield reviews;
            } default -> null;
        };
    }

}
