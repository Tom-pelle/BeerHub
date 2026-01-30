package it.unipi.BeerHub.service;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import it.unipi.BeerHub.model.Review;
import it.unipi.BeerHub.repository.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.springframework.stereotype.Service;
import it.unipi.BeerHub.config.neo4jDriverConfig;
import it.unipi.BeerHub.config.mongoDBdriverConfig;

import java.util.*;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;

@Service
public class reviewService {
    private final reviewRepository reviewRepository;
    MongoCollection<Document> collection;
    MongoCollection<Document> collectionB;
    MongoCollection<Document> collectionU;
    Driver graph_driver;

    /**
     * service constructor, initializes class objects
     * @param reviewRepository
     */
    public reviewService(reviewRepository reviewRepository){
        this.reviewRepository = reviewRepository;
        collection = mongoDBdriverConfig.getCollectionIst("reviews");
        collectionB = mongoDBdriverConfig.getCollectionIst("beers");
        collectionU = mongoDBdriverConfig.getCollectionIst("users");
        graph_driver = neo4jDriverConfig.getGraphDriver();
    }

    //CRUD operations
    public List<Review> getAll(){
        return reviewRepository.findAll();
    }

    public Optional<Review> findById(String id){return reviewRepository.findById(id);}

    public List<Review> findByIds(List<String> ids){return reviewRepository.findAllById(ids);}

    public Document deleteReview (String review_id){
        Optional<Review> a = reviewRepository.findById(review_id);
        if(!a.isPresent()){
            return new Document().append("review", "not found");
        }
        Document result = new Document();

        Document beer = collectionB.find(Filters.elemMatch("latestReviews", Filters.eq("review_id", review_id))).first();
        Document user = collectionU.find(Filters.eq("reviewIds", review_id)).first();

        Bson filter = Filters.or(
                Filters.elemMatch("latestReviews", Filters.eq("review_id", review_id)),
                Filters.eq("otherReviewIDs", review_id)
        );

        Bson update = Updates.combine(
                Updates.pull("latestReviews", new Document("review_id", review_id)),
                Updates.pull("otherReviewIDs", review_id)
        );

        UpdateResult docres = collectionB.updateOne(filter, update);
        result.append("updateBeer", docres.getModifiedCount());

        docres = collectionU.updateOne(
                Filters.eq("reviewIds", review_id),
                Updates.pull("reviewIds", review_id)
        );
        result.append("updateUser", docres.getModifiedCount());

        DeleteResult delres = collection.deleteOne(eq("_id", new ObjectId(review_id)));
        result.append("deleteRev", delres.getDeletedCount());

        if(beer != null && user != null){
            String beer_id = beer.getString("beer_id");
            String username = user.getString("username");

            Map<String, Object> param = new HashMap<>();
            param.put("beer_id", beer_id);
            param.put("username", username);
            String query =
                    "MATCH (u:User {username: $username})-[r:REVIEWED]->(b:Beer {beer_id: $beer_id})\n" +
                            "DELETE r;";

            try(var session = graph_driver.session()) {
                try(Transaction tx = session.beginTransaction()) {
                    Result res = tx.run(query, param);
                    result.append("graphDeleteCount", res.consume().counters().relationshipsDeleted());
                }
            }
        }else{
            result.append("graph", "not present on graph");
        }

        return result;
    }

    /**
     *
     * @param review documento proveniente dal front end quando un utente inserisce una review.
     *               L'inserimento avviene dalla pagina di una birra ed è quindi noto l'id della birra
     *               che è stata recensita.
     *               review:{
     *                  username: String        L'utente deve essere registrato per poter recensire
     *                  text: String            Opzionale, può anche essere vuoto
     *                  score: Double           Obbligatorio
     *                  beer_name: String
     *                  _id: String
     *                  date: ??? maybe
     *               }
     * @return       Documento con risultato dell'operazione e dell'aggiornamento
     */
    public Document insertReview(Document review){
        Document result = new Document();
        String beer_id = review.getString("beer_id");
        String username = review.getString("username");

        Document beerToUpdate = collectionB.find(eq("beer_id", beer_id)).first();
        Document userToUpdate = collectionU.find(eq("username", username)).first();

        if(beerToUpdate == null || userToUpdate == null){
            result.append("is beer present", beerToUpdate != null);
            result.append("is user present", userToUpdate != null);
            return result;
        }

        String beer_name = beerToUpdate.getString("beer_name");

        //Inserting document
        Document reviewToInsert = new Document().append("text", review.getString("text"))
                .append("beer_name", beer_name)
                .append("username", review.getString("username"))
                .append("date", review.getString("date"))
                .append("score", review.getDouble("score"));

        InsertOneResult insertResult = collection.insertOne(reviewToInsert);
        result.append("review doc insert", insertResult.wasAcknowledged());

        ObjectId objId = reviewToInsert.getObjectId("_id");
        String review_id = objId.toString();

        //Updating on Beers collection, with possible sliding of latestReviews array
        List<Document> latestReviews = beerToUpdate.getList("latestReviews", Document.class);
        List<String> otherReviewIDs = beerToUpdate.getList("otherReviewIDs", String.class);

        Document rev = new Document()
                .append("review_id", review_id)
                .append("score", review.getDouble("score"))
                .append("text", review.getString("text"))
                .append("username", review.getString("username"));

        UpdateResult beerUpdateRes = null;
        if(latestReviews == null){
            //beer hasn't reviews, field latestReviews can be simply added and sliding isn't necessary
            beerUpdateRes = collectionB.updateOne(
                    eq("beer_id", beer_id),
                    Updates.set("latestReviews", List.of(rev))
            );
        }else if(latestReviews.size() < 10){
            //latestReviews list isn't full and doesn't need sliding
            LinkedList<Document> LRlinked = new LinkedList<>(latestReviews);
            LRlinked.addFirst(rev);
            latestReviews = LRlinked;

            beerUpdateRes = collectionB.updateOne(
                    eq("beer_id", beer_id),
                    Updates.set("latestReviews", latestReviews)
            );
        }else if(latestReviews.size() >= 10){
            //latestReview list is full, sliding is necessary
            LinkedList<Document> LRlinked = new LinkedList<>(latestReviews);
            LRlinked.addFirst(rev);

            if(otherReviewIDs == null){
                otherReviewIDs = new ArrayList<>();
                otherReviewIDs.add(LRlinked.getLast().getString("review_id"));
            }else {
                otherReviewIDs.add(LRlinked.getLast().getString("review_id"));
            }

            LRlinked.removeLast();
            latestReviews = LRlinked;

            beerUpdateRes = collectionB.updateOne(
                    eq("beer_id", beer_id),
                    Updates.combine(
                        Updates.set("latestReviews", latestReviews),
                        Updates.set("otherReviewIDs", otherReviewIDs)
                    )
            );
        }
        result.append("beer update", beerUpdateRes.wasAcknowledged());

        //Update of user document
        List<String> reviewIds = userToUpdate.getList("reviewIds", String.class);
        if(reviewIds == null){
            reviewIds = new ArrayList<>();
            reviewIds.add(review_id);
        }else{
            reviewIds.add(review_id);
        }

        UpdateResult userUpdateRes = collectionU.updateOne(
                eq("username", username),
                Updates.set("reviewIds", reviewIds)
        );
        result.append("user update", userUpdateRes.wasAcknowledged());

        //Adding reviewed relationship to graph db
        Map<String, Object> params = new HashMap<>();
        params.put("beer_id", beer_id);
        params.put("username", username);
        params.put("score", review.getDouble("score"));

        String query =
                "MATCH (u:User {username: $username}), (b:Beer {beer_id: $beer_id})\n" +
                "CREATE (u)-[:REVIEWED {score: $score}]->(b)";

        try(var session = graph_driver.session()){
            try(Transaction tx = session.beginTransaction()){
                Result res = tx.run(query, params);
                result.append("graph update", res.consume().counters().relationshipsCreated());
                tx.commit();
            }catch(Exception e){
                result.append("graph error", e.getMessage());
            }
        }catch(Exception e){
            result.append("graph error", e.getMessage());
        }

        return result;
    }

    //Statistc with mongo aggregations
    public List<Document> getTopActiveUsersStats(int limit) {
        List<Bson> pipeline = new ArrayList<>();

        // 1) filtro score valido (toglie null e valori fuori range)
        pipeline.add(match(and(gte("score", 1), lte("score", 5))));

        // 2) group per username: count, avg e distribuzione 1..5
        pipeline.add(group("$username",
                Accumulators.sum("reviewsCount", 1),
                Accumulators.avg("avgScoreGiven", "$score"),
                Accumulators.sum("s1", new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$score", 1)), 1, 0))),
                Accumulators.sum("s2", new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$score", 2)), 1, 0))),
                Accumulators.sum("s3", new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$score", 3)), 1, 0))),
                Accumulators.sum("s4", new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$score", 4)), 1, 0))),
                Accumulators.sum("s5", new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$score", 5)), 1, 0)))
        ));

        // 3) sort: più attivi, poi media più alta
        pipeline.add(sort(orderBy(
                descending("reviewsCount"),
                descending("avgScoreGiven")
        )));

        // 4) limit
        pipeline.add(limit(limit));

        // 5) project: rinomina _id -> username e crea ratingDist
        pipeline.add(project(fields(
                excludeId(),
                computed("username", "$_id"),
                include("reviewsCount"),
                computed("avgScoreGiven", new Document("$round", Arrays.asList("$avgScoreGiven", 2))),
                computed("ratingDist", new Document()
                        .append("1", "$s1")
                        .append("2", "$s2")
                        .append("3", "$s3")
                        .append("4", "$s4")
                        .append("5", "$s5"))
        )));

        List<Document> results = new ArrayList<>();
        collection.aggregate(pipeline).into(results);
        return results;
    }

    public List<Document> getTop15ActiveUsersStats() {
        return getTopActiveUsersStats(15);
    }

}