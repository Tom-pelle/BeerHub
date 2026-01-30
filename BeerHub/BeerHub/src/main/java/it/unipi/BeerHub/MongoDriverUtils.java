package it.unipi.BeerHub;

import com.mongodb.client.*;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

public class MongoDriverUtils {
    private static final String PROTOCOL = "mongodb://";
    private static final String MONGO_HOST = "100.71.188.41";
    private static final String MONGO_PORT = "27017";
    private static final String MONGO_DB = "BeerHub";
    private static final String MONGO_USER = "marco";
    private static final String MONGO_PSW = "marco";

    public static MongoClient getConnection() {
        String connectionString =String.format("%s%s:%s@%s:%s/%s", PROTOCOL, MONGO_USER,
                MONGO_PSW, MONGO_HOST, MONGO_PORT, MONGO_DB);
        return MongoClients.create(connectionString);
    }

    public static void listCollectionNames(MongoClient myClient, MongoDatabase db) {
        MongoIterable<String> existingCollections = db.listCollectionNames();
        System.out.println("\n////////////// Currently existing databases are: //////////////");
        for (String collection : existingCollections) {
            System.out.println("Collection: " + collection);
        }
        System.out.println("///////////////////////////////////////////////////////////////");
    }

    public static void createCollection(MongoDatabase db, String collectionName) {
        try {
            System.out.println("\n////////////// Creating new Collection: [" + collectionName + "] //////////////");
            db.createCollection(collectionName);
        } catch ( Exception e) {
            System.out.println("The collection you are trying to create already exists.");
        }
    }

    public static void dropCollection(MongoDatabase db, String collectionName) {
        System.out.println("\n////////////// Dropping Collection: [" + collectionName + "] //////////////");
        MongoCollection<Document> collectionToDel = db.getCollection(collectionName);
        collectionToDel.drop();
    }

    public static void insertDocuments(MongoCollection<Document> dstCollection, List<Document> documents) {
        String collectionName = dstCollection.getNamespace().getCollectionName();
        System.out.println("\n////////////// Inserting Documents into Collection: [" + collectionName + "] //////////////");
    if (documents.size() == 1) {
            dstCollection.insertOne(documents.get(0));
        }
        if (documents.size() > 1) {
            InsertManyResult insertResults = dstCollection.insertMany(documents);

            List<ObjectId> insertedIds = new ArrayList<>();
            insertResults.getInsertedIds().values()
                    .forEach(doc -> insertedIds.add(doc.asObjectId().getValue()));

            //System.out.println("Inserted documents with the following ids: " + insertedIds);
        }
        System.out.println("///////////////////////////////////////////////////////////////");
    }

    public static void updateDocuments(MongoCollection<Document> dstCollection, Bson matchFilter, Bson updateFilter) {
        System.out.println("\n////////////// Updating Documents //////////////");
        UpdateResult updateResult = dstCollection.updateMany(matchFilter, updateFilter);
        System.out.println("Updated " + updateResult.getModifiedCount() + " docuemnts.");
        System.out.println("///////////////////////////////////////////////////////////////");
    }
}

