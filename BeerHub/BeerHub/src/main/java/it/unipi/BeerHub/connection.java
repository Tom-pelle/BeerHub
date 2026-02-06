package it.unipi.BeerHub;

import com.mongodb.client.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.opencsv.CSVWriter;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

//bolt://127.0.0.1:7687
public class connection {
    Driver driver;
    String uri = "bolt://127.0.0.1:7687";
    public void main(String[] args) {
        /*var config = Config.builder()
                .withMaxConnectionLifetime(90, TimeUnit.MINUTES)
                .withMaxConnectionPoolSize(50)
                .withConnectionAcquisitionTimeout(2, TimeUnit.MINUTES)
                .build();
        driver = GraphDatabase.driver(uri, AuthTokens.basic("neo4j", "marcolari"), config);



        Map<String, Object> params = new HashMap<>();
        params.put("username", "Walnut");

        String query =
                "MATCH (u:User) WHERE u.username=$username return u";

        try(var session = driver.session(SessionConfig.builder().withDatabase("neo4j").build());){
            try(Transaction tx = session.beginTransaction()){
                Result result = tx.run(query, params);
                if (result.hasNext()) {
                    // Get the node 'u' returned by the query
                    Node userNode = result.next().get("u").asNode();

                    // Now print the properties of the node (example: username)
                    System.out.println("Username: " + userNode.get("username").asString());
                    System.out.println("Other properties: " + userNode.asMap());
                } else {
                    System.out.println("No user found with username 'Walnut'");
                }
            }

        }*/
        MongoClient mongoClient = MongoClients.create("mongodb://marco:marco@100.71.188.41:27017");
        MongoDatabase database = mongoClient.getDatabase("BeerHub");  // Sostituisci con il nome del tuo DB
        MongoCollection<Document> collection = database.getCollection("breweries");

        // Creazione della pipeline di aggregazione con il $match per controllare featuredBeers
        List<Bson> pipeline = Arrays.asList(
                // Filtro per assicurarsi che featuredBeers esista, non sia null, e sia un array
                Aggregates.match(Filters.and(
                        Filters.exists("featuredBeers", true),   // Assicurati che featuredBeers esista
                        Filters.ne("featuredBeers", null)   // Assicurati che featuredBeers non sia nu// Assicurati che featuredBeers sia un array (tipo 4)
                )),
                // Proietta i campi desiderati e calcola la dimensione dell'array featuredBeers
                Aggregates.project(Projections.fields(
                        Projections.include("name", "brewery_id"),
                        Projections.computed("featuredBeersCount", new Document("$size", "$featuredBeers"))  // Conta gli elementi in featuredBeers
                )),
                // Ordina per la quantità di featuredBeersCount in ordine decrescente
                Aggregates.sort(new Document("featuredBeersCount", -1)),
                // Limita ai primi 1000 risultati
                Aggregates.limit(1000)
        );
        String filePath = "breweries_top_1000.csv";
        // Esegui l'aggregazione
        MongoCursor<Document> cursor = collection.aggregate(pipeline).iterator();

        // Scrivere i risultati in un file CSV
        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath))) {
            // Scrivi l'intestazione
            writer.writeNext(new String[]{"brewery_id", "name", "featuredBeersCount"});
            System.out.println("Inizio scrittura dei dati...");

            // Scrivi i dati
            int count = 0;
            while (cursor.hasNext()) {
                Document brewery = cursor.next();
                String brewery_id = brewery.getString("brewery_id");
                String name = brewery.getString("name");
                int featuredBeersCount = brewery.getInteger("featuredBeersCount", 0); // Default a 0 se non trovato
                writer.writeNext(new String[]{brewery_id, name, String.valueOf(featuredBeersCount)});
                count++;
            }

            // Stampa il numero di righe scritte
            System.out.println("Scritti " + count + " record nel file CSV.");

        } catch (IOException e) {
            System.err.println("Errore durante la scrittura del file CSV.");
            e.printStackTrace();
        } finally {
            mongoClient.close();
            System.out.println("Connessione al database MongoDB chiusa.");
        }
    }
}