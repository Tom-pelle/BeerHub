package it.unipi.BeerHub;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.csv.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.nio.file.*;

import static it.unipi.BeerHub.MongoDriverUtils.*;

//I CSV SONO STATI IMPORTATI SU MONGODB
public class CSVImporter {
    private static final int BATCH_SIZE = 5000;
    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private static long insertBreweries() {
        //variabili per fare rapporto sullo stato dell'inserimento
        long total = 0;
        long badRows = 0;

        List<Document> batch = new ArrayList<>(BATCH_SIZE);
        //connessione al database BeerHub su raspberry 100.71.188.41
        MongoCollection<Document> collection = database.getCollection("breweries");

        //path assoluto della directory con i datasets
        Path p = Path.of("C:/Users/marco/Desktop/BeerHub/datasets/breweries_reduced.csv").toAbsolutePath().normalize();

        //blocco try per la sicurezza sul reader del file csv
        try(Reader in = new FileReader(p.toFile());){
            System.out.println("Reader aperto con successo");

            Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setTrim(true)
                    .setDelimiter(';')
                    .build()
                    .parse(in);

            //costruzione dell'array di documenti per l'inserimento
            for (CSVRecord record : records) {
                total++;
                try {
                    // Opzione A: prendi tutto l'header->value e inserisci tutto
                    Map<String, String> row = record.toMap();

                    Document doc = new Document();
                    for (var e : row.entrySet()) {
                        doc.append(e.getKey(), e.getValue()); // oppure smartParse(e.getValue())
                    }

                    // Opzione B: se vuoi campi specifici
                    // doc.append("lastName", record.get("Last Name"));
                    // doc.append("firstName", record.get("First Name"));

                    batch.add(doc);

                } catch (Exception ex) {
                    badRows++;
                }

                if (batch.size() >= BATCH_SIZE) {
                    insertDocuments(collection, batch);
                    batch.clear();
                    System.out.printf("Progress: total=%d bad=%d%n", total, badRows);
                }
            }
            if (!batch.isEmpty()) {
                insertDocuments(collection, batch);
                batch.clear();
                System.out.printf("Progress: total=%d bad=%d%n", total, badRows);
            }
        }catch(Exception e){
            System.out.println("Errore " + e.getMessage());
        }
        mongoClient.close();
        return total;
    }

    private static long insertBeers(){
        //variabili per fare rapporto sullo stato dell'inserimento
        long total = 0;
        long badRows = 0;

        List<Document> batch = new ArrayList<>(BATCH_SIZE);
        //connessione al database BeerHub su raspberry 100.71.188.41
        MongoCollection<Document> collection = database.getCollection("beers");

        //path assoluto della directory con i datasets
        Path p = Path.of("C:/Users/marco/Desktop/BeerHub/datasets/beers_completo.csv").toAbsolutePath().normalize();

        try(Reader in = new FileReader(p.toFile());){
            System.out.println("Reader aperto con successo");

            Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setTrim(true)
                    .setDelimiter(';')
                    .build()
                    .parse(in);

            //costruzione dell'array di documenti per l'inserimento
            for (CSVRecord record : records) {
                total++;
                try {
                    // Opzione A: prendi tutto l'header->value e inserisci tutto
                    Map<String, String> row = record.toMap();

                    Document doc = new Document();
                    for (var e : row.entrySet()) {
                        doc.append(e.getKey(), e.getValue()); // oppure smartParse(e.getValue())
                    }

                    // Opzione B: se vuoi campi specifici
                    // doc.append("lastName", record.get("Last Name"));
                    // doc.append("firstName", record.get("First Name"));

                    batch.add(doc);

                } catch (Exception ex) {
                    badRows++;
                }

                if (batch.size() >= BATCH_SIZE) {
                    insertDocuments(collection, batch);
                    batch.clear();
                    System.out.printf("Progress: total=%d bad=%d%n", total, badRows);
                }
            }
            if (!batch.isEmpty()) {
                insertDocuments(collection, batch);
                batch.clear();
                System.out.printf("Progress: total=%d bad=%d%n", total, badRows);
            }
        }catch(Exception e){
            System.out.println("Errore " + e.getMessage());
        }
        return total;
    }

    private static void insertReviews(){
        //variabili per fare rapporto sullo stato dell'inserimento
        long total = 0;
        long badRows = 0;

        List<Document> batch = new ArrayList<>(BATCH_SIZE);
        //connessione al database BeerHub su raspberry 100.71.188.41
        MongoCollection<Document> collection = database.getCollection("reviews");

        //path assoluto della directory con i datasets
        Path p = Path.of("C:/Users/marco/Desktop/BeerHub/datasets/reviews.csv").toAbsolutePath().normalize();

        try(Reader in = new FileReader(p.toFile());){
            System.out.println("Reader aperto con successo");

            Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setTrim(true)
                    .setDelimiter(';')
                    .build()
                    .parse(in);

            //costruzione dell'array di documenti per l'inserimento
            for (CSVRecord record : records) {
                total++;
                try {
                    // Opzione A: prendi tutto l'header->value e inserisci tutto
                    Map<String, String> row = record.toMap();

                    Document doc = new Document();
                    for (var e : row.entrySet()) {
                        doc.append(e.getKey(), e.getValue()); // oppure smartParse(e.getValue())
                    }

                    // Opzione B: se vuoi campi specifici
                    // doc.append("lastName", record.get("Last Name"));
                    // doc.append("firstName", record.get("First Name"));

                    batch.add(doc);

                } catch (Exception ex) {
                    badRows++;
                }

                if (batch.size() >= BATCH_SIZE) {
                    insertDocuments(collection, batch);
                    batch.clear();
                    System.out.printf("Progress: total=%d bad=%d%n", total, badRows);
                }
            }
            if (!batch.isEmpty()) {
                insertDocuments(collection, batch);
                batch.clear();
                System.out.printf("Progress: total=%d bad=%d%n", total, badRows);
            }
        }catch(Exception e){
            System.out.println("Errore " + e.getMessage());
        }
    }

    private static void insertUsers(){
        //variabili per fare rapporto sullo stato dell'inserimento
        long total = 0;
        long badRows = 0;

        List<Document> batch = new ArrayList<>(BATCH_SIZE);
        //connessione al database BeerHub su raspberry 100.71.188.41
        MongoCollection<Document> collection = database.getCollection("users");

        //path assoluto della directory con i datasets
        Path p = Path.of("C:/Users/marco/Desktop/BeerHub/datasets/users.csv").toAbsolutePath().normalize();

        try(Reader in = new FileReader(p.toFile());){
            System.out.println("Reader aperto con successo");

            Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setTrim(true)
                    .setDelimiter(',')
                    .build()
                    .parse(in);

            //costruzione dell'array di documenti per l'inserimento
            for (CSVRecord record : records) {
                total++;
                try {
                    // Opzione A: prendi tutto l'header->value e inserisci tutto
                    Map<String, String> row = record.toMap();

                    Document doc = new Document();
                    for (var e : row.entrySet()) {
                        doc.append(e.getKey(), e.getValue()); // oppure smartParse(e.getValue())
                    }

                    // Opzione B: se vuoi campi specifici
                    // doc.append("lastName", record.get("Last Name"));
                    // doc.append("firstName", record.get("First Name"));

                    batch.add(doc);

                } catch (Exception ex) {
                    badRows++;
                }

                if (batch.size() >= BATCH_SIZE) {
                    insertDocuments(collection, batch);
                    batch.clear();
                    System.out.printf("Progress: total=%d bad=%d%n", total, badRows);
                }
            }
            if (!batch.isEmpty()) {
                insertDocuments(collection, batch);
                batch.clear();
                System.out.printf("Progress: total=%d bad=%d%n", total, badRows);
            }
        }catch(Exception e){
            System.out.println("Errore " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        mongoClient = MongoClients.create("mongodb://marco:marco@100.71.188.41:27017");
        database = mongoClient.getDatabase("BeerHub");

        //insertBreweries();
        // insertBeers();
        //insertReviews();
        //insertUsers();

        //I CSV SONO STATI IMPORTATI SU MONGODB
    }
}
