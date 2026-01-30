package it.unipi.BeerHub.service;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import it.unipi.BeerHub.model.Beer;
import it.unipi.BeerHub.model.Review;
import it.unipi.BeerHub.repository.*;
import it.unipi.BeerHub.config.neo4jDriverConfig;
import it.unipi.BeerHub.config.mongoDBdriverConfig;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Result;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;
import static org.springframework.expression.common.ExpressionUtils.toDouble;

@Service
public class beerService {
    private final beerRepository beerRepository;
    MongoCollection<Document> collection;
    MongoCollection<Document> collectionR;
    MongoCollection<Document> collectionBr;
    Driver graph_driver;

    /**
     * service constructor, initializes class objects
     * @param beerRepository
     */
    public beerService(beerRepository beerRepository){
        this.beerRepository = beerRepository;
        collection = mongoDBdriverConfig.getCollectionIst("beers");
        collectionR = mongoDBdriverConfig.getCollectionIst("reviews");
        collectionBr = mongoDBdriverConfig.getCollectionIst("breweries");
        graph_driver = neo4jDriverConfig.getGraphDriver();
    }

    //Various CRUD operation

    public List<Beer> getAll() {
        return beerRepository.findAll();
    }

    /**
     * Get by beer_id and not _id
     * @param id beer_id
     * @return Beer entity

    public Beer getById(String id) {
        Document doc = collection.find(eq("beer_id", id)).first();
        Beer beer = new Beer();

        beer.setBeer_id(doc.getString("beer_id"));
        beer.setName(doc.getString("name"));
        beer.setStyle(doc.getString("style"));
        beer.setAbv(doc.getDouble("abv"));
        beer.setBrewery_name(doc.getString("brewery_name"));
        beer.setCountry(doc.getString("country"));

        return beer;
    }*/
    public Optional<Beer> findById(String id) {return beerRepository.findById(id);}

    // Method to find a beer by its exact name
    public Beer getByName(String name) {
        // Search for the document matching the beer name
        Document doc = collection.find(eq("name", name)).first();
        if (doc == null) return null;

        Beer beer = new Beer();
        beer.setBeer_id(doc.getString("beer_id"));
        beer.setName(doc.getString("name"));
        return beer;
    }

    public List<String> getReviewsIDs(String beerId) {
        List<Bson> pipeline = List.of(
                match(eq("beer_id", beerId)),
                match(exists("latestReviews")),
                new Document("$set", new Document("allReviewsIds",
                        new Document("$setUnion", Arrays.asList(
                                new Document("$ifNull", Arrays.asList(
                                        new Document("$map", new Document()
                                                .append("input", new Document("$ifNull", Arrays.asList("$latestReviews", List.of())))
                                                .append("as", "r")
                                                .append("in", "$$r.review_id")
                                        ),
                                        List.of()
                                )),
                                new Document("$ifNull", Arrays.asList("$otherReviewIDs", List.of()))
                        )
                        )
                )
                )
        );
        return collection.aggregate(pipeline).first().getList("allReviewsIds", String.class);
    }
    /**
     * Admin function to update a beer document and relationships
     * @param beerId
     * @param updateData a document with the fields keys and the new values
     * @return A document containig information about the update operation
     */
    public Document updateBeer(String beerId, Document updateData) {
        List<Bson> updates = new ArrayList<>();
        UpdateResult resBeer = null;
        UpdateResult resRev = null;
        UpdateResult resBr = null;

        // Mapping fields based on your MongoDB structure
        if (updateData.containsKey("name")){ //Oltre a cambiare il nome nel documento della birra va cambiato anche nelle reviews
            updates.add(Updates.set("name", updateData.getString("name")));
            //Update in reviews
            List<Bson> revUpdates = new ArrayList<>();
            revUpdates.add(Updates.set("beer_name", updateData.getString("name")));

            List<String> r_ids = getReviewsIDs(beerId);
            List<ObjectId> objIds = r_ids.stream()
                    .map(ObjectId::new)
                    .toList();
            resRev = collectionR.updateMany(in("_id", objIds), Updates.combine(revUpdates));
            //Cambio del nome anche nelle featured beers in brewery
            resBr = collectionBr.updateOne(
                    eq("featuredBeers.beer_id", beerId),
                    Updates.set("featuredBeers.$.name", updateData.getString("name"))
            );
        }
        if (updateData.containsKey("style")) updates.add(Updates.set("style", updateData.getString("style")));
        if (updateData.containsKey("abv")) updates.add(Updates.set("abv", updateData.get("abv")));
        if (updateData.containsKey("country")) updates.add(Updates.set("country", updateData.getString("country")));
        if (updateData.containsKey("brewery_name")){
            updates.add(Updates.set("brewery_name", updateData.getString("brewery_name")));
        }

        if (updates.isEmpty()) return null;
        // Filter by the custom beer_id field
        resBeer = collection.updateOne(eq("beer_id", beerId), Updates.combine(updates));

        StringBuilder query = new StringBuilder(
                "MATCH (b:Beer {beer_id: $beerId}) "
        );

        Map<String, Object> params = new HashMap<>();
        List<String> sets = new ArrayList<>();
        params.put("beerId", beerId);
        if((updateData.containsKey("style"))){
            params.put("style", updateData.getString("style"));
            sets.add("b.style = $style");
        }
        if((updateData.containsKey("name"))){
            params.put("name", updateData.getString("name"));
            sets.add("b.name = $name");
        }
        if((updateData.containsKey("country"))){
            params.put("country", updateData.getString("country"));
            sets.add("b.country = $country");
        }
        Result res = null;
        try (var session = graph_driver.session()) {
            query.append("SET ").append(String.join(", ", sets));
            System.out.println(query.toString());
            System.out.println(params);
            try(Transaction tx = session.beginTransaction()) {
                res = tx.run(query.toString(), params);
                tx.commit();
            }
        }
        Document toReturn = new Document();
        toReturn.put("matchDocBeer", resBeer.getMatchedCount());
        toReturn.put("updateDocBeer", resBeer.getModifiedCount());
        if(updateData.containsKey("name")){
            toReturn.put("matchDocRev", resRev.getMatchedCount());
            toReturn.put("updateDocRev", resRev.getModifiedCount());
            toReturn.put("matchDocBr", resBr.getMatchedCount());
            toReturn.put("updateDocBr", resBr.getModifiedCount());
        }
        toReturn.put("graphUpdate", res.consume().counters().propertiesSet());
        return toReturn;
    }

    /**
     *  Admin function to delete a beer document
     * @param name
     * @return Status document
     */
    public Document deleteByBeerName(String name) {
        // First, we find the beer to get its beer_id
        Beer b = getByName(name);
        Document result = new Document();
        if (b == null) {
            result.append("beer", "not found");
            return result; // Beer not found
        }

        // Execute deletion based on the beer_id
        DeleteResult docres = collection.deleteOne(eq("beer_id", b.getBeer_id()));
        result.append("docDelCount", docres.getDeletedCount());

        UpdateResult upres = collectionBr.updateOne(
                Filters.eq("featuredBeers.beer_id", b.getBeer_id()),
                Updates.pull("featuredBeers",
                        new Document("beer_id", b.getBeer_id())
                )
        );
        result.append("updateDocBeer", upres.getModifiedCount());
        Map<String, Object> params = new HashMap<>();
        params.put("beer_id", b.getBeer_id());
        String query =
                "MATCH (b:Beer {beer_id: $beer_id})\n" +
                        "DETACH DELETE b;";

        try (var session = graph_driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                Result graphres = tx.run(query, params);
                result.append("graphDelCount", graphres.consume().counters().nodesDeleted() + graphres.consume().counters().relationshipsDeleted());
                tx.commit();
            }
        }

        return result;
    }

    /**
     * Admin function to insert a beer
     * @param beerData Document ready to be inserted
     * @return Status string
     */
    public String insertBeer(Document beerData) {
        String beerId = beerData.getString("beer_id");

        // 1. If beer_id is missing, generate a new one automatically
        if (beerId == null || beerId.isEmpty()) {
            // Find the document with the highest beer_id.
            Document maxBeer = collection.find()
                    .sort(descending("beer_id"))
                    .first();

            if (maxBeer != null) {
                try {
                    // Parse the current maximum ID to a long value
                    long maxIdVal = Long.parseLong(maxBeer.getString("beer_id"));
                    long nextId = maxIdVal + 1;

                    // --- COLLISION SAFETY CHECK ---
                    // Due to lexicographical sorting, we might have skipped some IDs.
                    // We increment the counter until we find a beer_id that is truly available.
                    while (collection.countDocuments(eq("beer_id", String.valueOf(nextId))) > 0) {
                        nextId++;
                    }

                    beerId = String.valueOf(nextId);
                } catch (NumberFormatException e) {
                    // Fallback: Use current timestamp if beer_id is not a valid numeric string
                    beerId = String.valueOf(System.currentTimeMillis() / 1000);
                }
            } else {
                // Default ID for the very first document in the collection
                beerId = "1";
            }
            beerData.append("beer_id", beerId);
        } else {
            // 2. If a manual ID is provided, verify its uniqueness
            if (collection.countDocuments(eq("beer_id", beerId)) > 0) {
                return "Error: A beer with ID " + beerId + " already exists.";
            }
        }

        // 4. Perform the insertion into MongoDB
        String result = "";
        try {
            collection.insertOne(beerData);
            result += "Beer '" + beerData.getString("name") + "' inserted successfully with ID: " + beerId;
        } catch (Exception e) {
            // Catch any database-level exceptions (e.g., unique index violations)
            result += "Critical DB error during insertion: " + e.getMessage();
        }



        Map<String, Object> params = new HashMap<>();
        params.put("beer_id", beerId);
        params.put("name", beerData.getString("name"));
        params.put("style", beerData.getString("style"));
        params.put("country", beerData.getString("country"));
        params.put("brewery_name", beerData.getString("brewery_name"));

        String query =
                "MERGE (b:Beer {beer_id: $beer_id})\n" +
                        "SET  b.name  = $name,\n" +
                        "     b.style = $style,\n" +
                        "     b.country   = $country\n";

        try (var session = graph_driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                tx.run(query, params);
                tx.commit();
            }
        }
        return result;
    }

    //Statistics functions with complex Mongo aggregation

    /**
     * Browsing function.
     * @param name optional
     * @param style optional
     * @param country optional
     *          parameters from browsing request. At least one is needed
     * @return Beer entity list, evaluating given parameters
     */
    public List<Beer> FilteredGet(String name, String style, String country) {
        List<Bson> pipeline = new ArrayList<>();
        if(name != null) {
            pipeline.add(match(text(name)));
            pipeline.add(
                    project(fields(
                            include("_id", "name", "abv", "brewery_name", "country", "style"),
                            metaTextScore("score")
                    ))
            );
            pipeline.add(match(regex("name", Pattern.compile(name, Pattern.CASE_INSENSITIVE))));
        }
        if(country != null) {pipeline.add(match(regex("country", Pattern.compile(country, Pattern.CASE_INSENSITIVE))));}
        if(style != null) {pipeline.add(match(regex("style", Pattern.compile(style, Pattern.CASE_INSENSITIVE))));}
        if(name!=null){pipeline.add(sort(orderBy(metaTextScore("score"), ascending("_id"))));}
        else{pipeline.add(sort(ascending("_id")));}
        pipeline.add(limit(50));

        List<Document> results = collection.aggregate(pipeline).into(new ArrayList<>());

        List<Beer> beers = new ArrayList<>();

        for (Document d : results) {
            Beer b = new Beer();
            b.setId(d.getObjectId("_id").toHexString());
            b.setName(d.getString("name"));
            b.setStyle(d.getString("style"));
            b.setAbv(d.getDouble("abv"));
            b.setCountry(d.getString("country"));
            b.setBrewery_name(d.getString("brewery_name"));
            beers.add(b);
        }
        return beers;

    }

    //Da id a avg_Score di reviews
    public Document AvgScore(String id){
        List<Bson> pipeline = new ArrayList<>();

        Document mapLatest = new Document("$map", new Document()
                .append("input", "$latestReviews")
                .append("as", "r")
                .append("in", "$$r.review_id")
        );

        Document reviewsIdsExpr = new Document("$setUnion", Arrays.asList(
                mapLatest,
                "$otherReviewIDs"
        ));

        ObjectId objectId = new ObjectId(id);
        pipeline.add(match(eq("_id", objectId)));
        pipeline.add(project(fields(include("beer_id"), computed("reviews_ids", reviewsIdsExpr))));

        Document d = collection.aggregate(pipeline).first();
        if(d == null) {return new Document().append("avgScore", -1.00);}
        List<String> review_ids = d.getList("reviews_ids", String.class);
        if(review_ids == null || review_ids.isEmpty()) {return new Document().append("avgScore", 0.00).append("count", 0);}

        List<ObjectId> ids = review_ids.stream()
                .filter(ObjectId::isValid)
                .map(ObjectId::new)
                .toList();

        pipeline.clear();
        pipeline.add(match(in("_id", ids)));
        pipeline.add(match(and(
                exists("score", true),
                ne("score", ""),
                ne("score", null)
        )));
        pipeline.add(addFields(new Field<>("scoreD", new Document("$toDouble", "$score"))));
        pipeline.add(group(
                null,
                Accumulators.avg("avgScore", "$scoreD"),
                Accumulators.sum("count", 1)
        ));
        pipeline.add(project(fields(
                include("avgScore", "count")
        )));

        return collectionR.aggregate(pipeline).first();
    }

    /**
     * Finds rising/falling beer trends based on latestReviews score comparison.
     * Calculates percentage change between recent (last half) vs older (first half) reviews.
     * Positive trend = recentAvg > olderAvg. Negative trend = recentAvg < olderAvg.
     *
     * @param country optional exact country filter
     * @param style optional case-insensitive style filter
     * @param positiveTrend true=show rising (>minTrend%), false=show falling (<-minTrend%) (default true)
     * @param minTrend minimum trend threshold in % (default 5.0)
     * @return List of beer documents with trend percentage (rounded to 2 decimals)
     */
    public List<Document> getTrends(String country, String style, boolean positiveTrend, double minTrend) {
        List<Bson> pipeline = new ArrayList<>();

        // optional filters for country (exact match) and style (case-insensitive regex)
        if (country != null && !country.isEmpty()) {
            pipeline.add(match(eq("country", country)));
        }
        if (style != null && !style.isEmpty()) {
            pipeline.add(match(regex("style", Pattern.compile(style, Pattern.CASE_INSENSITIVE))));
        }

        // filter beers with at least 6 recent reviews (index 5 exists)
        pipeline.add(match(exists("latestReviews.5", true)));

        // filter out reviews without score and keep essential beer fields
        pipeline.add(project(fields(
                include("beer_id", "name", "style", "country", "brewery_name", "abv"),
                computed("latestReviews", new Document("$filter", new Document()
                        .append("input", "$latestReviews")
                        .append("as", "review")
                        .append("cond", new Document("$ne", Arrays.asList("$$review.score", "")))
                ))
        )));

        // split latestReviews into recent (last half) and older (first half) using $slice
        pipeline.add(project(fields(
                include("beer_id", "name", "style", "country", "brewery_name", "abv", "latestReviews"),
                computed("recentReviews", new Document("$slice", Arrays.asList(
                        "$latestReviews",
                        new Document("$floor", new Document("$divide", Arrays.asList(new Document("$size", "$latestReviews"), 2)))
                ))),
                computed("olderReviews", new Document("$slice", Arrays.asList(
                        "$latestReviews",
                        new Document("$floor", new Document("$divide", Arrays.asList(new Document("$size", "$latestReviews"), 2))),
                        new Document("$size", "$latestReviews")
                )))
        )));

        // calculate average scores for recent and older reviews, converting score strings to double
        pipeline.add(addFields(
                new Field<>("recentAvg", new Document("$avg", new Document("$map", new Document()
                        .append("input", "$recentReviews")
                        .append("as", "review")
                        .append("in", new Document("$toDouble", "$$review.score"))
                ))),
                new Field<>("olderAvg", new Document("$avg", new Document("$map", new Document()
                        .append("input", "$olderReviews")
                        .append("as", "review")
                        .append("in", new Document("$toDouble", "$$review.score"))
                )))
        ));

        // calculate raw trend ratio (recentAvg/olderAvg - 1 for positive, olderAvg/recentAvg - 1 for negative)
        pipeline.add(addFields(new Field<>("trendRaw", new Document("$cond", new Document()
                .append("if", new Document("$gt", Arrays.asList("$recentAvg", "$olderAvg")))
                .append("then", new Document("$divide", Arrays.asList(
                        new Document("$subtract", Arrays.asList("$recentAvg", "$olderAvg")), "$olderAvg")))
                .append("else", new Document("$multiply", Arrays.asList(
                        new Document("$divide", Arrays.asList(
                                new Document("$subtract", Arrays.asList("$olderAvg", "$recentAvg")), "$recentAvg")), -1.0)))
        ))));

        // convert raw trend to percentage and round to 2 decimal places
        pipeline.add(addFields(new Field<>("trend", new Document("$round", Arrays.asList(
                new Document("$multiply", Arrays.asList("$trendRaw", 100)),  // Convert to percentage
                2  // Round to 2 decimal places
        )))));

        // filter by trend direction (positive > minTrend% or negative < -minTrend%)
        Bson trendFilter = positiveTrend ? gt("trend", minTrend) : lt("trend", -minTrend);
        pipeline.add(match(trendFilter));

        // final projection excluding MongoDB _id field, keeping only requested beer fields + trend
        pipeline.add(project(fields(
                excludeId(),  // Explicitly exclude MongoDB ObjectId (_id field)
                include("beer_id", "name", "style", "country", "brewery_name", "abv", "trend")
        )));

        pipeline.add(sort(descending("trend")));

        pipeline.add(limit(10));
        return collection.aggregate(pipeline).into(new ArrayList<>());
    }

    public Document CountryBeerStyles(String country, int topK) {
        List<Bson> pipeline = new ArrayList<>();

        if(topK < 1) topK = 1;

        // 1) solo birre del paese richiesto
        pipeline.add(match(eq("country", country)));

        // 2) group per style: count + avg(abv)
        pipeline.add(group("$style",
                Accumulators.sum("beerCount", 1),
                Accumulators.avg("avgAbvStyle", "$abv")
        ));

        // 3) ordina per diffusione
        pipeline.add(sort(descending("beerCount")));

        // 4) group finale: totalBeers, distinctStyles, array styles
        pipeline.add(group(null,
                Accumulators.sum("totalBeers", "$beerCount"),
                Accumulators.sum("distinctStyles", 1),
                Accumulators.push("styles", new Document("style", "$_id")
                        .append("beerCount", "$beerCount")
                        .append("avgAbvStyle", new Document("$round", Arrays.asList("$avgAbvStyle", 2)))
                )
        ));

        // 5) project: topStyles, topKBeers
        pipeline.add(project(new Document("_id", 0)
                .append("country", country)
                .append("totalBeers", 1)
                .append("distinctStyles", 1)
                .append("topStyles", new Document("$slice", Arrays.asList("$styles", topK)))
                .append("topKBeers",
                        new Document("$sum",
                                new Document("$slice", Arrays.asList("$styles.beerCount", topK))
                        )
                )
        ));

        // 6) percentuale copertura topK sul totale
        pipeline.add(addFields(new Field<>("topKCoveragePercent",
                new Document("$round", Arrays.asList(
                        new Document("$multiply", Arrays.asList(
                                new Document("$divide", Arrays.asList("$topKBeers", "$totalBeers")),
                                100
                        )),
                        2
                ))
        )));

        Document res = collection.aggregate(pipeline).first();
        if(res == null) {
            // paese non presente / nessuna birra
            return new Document("country", country)
                    .append("totalBeers", 0)
                    .append("distinctStyles", 0)
                    .append("topStyles", List.of())
                    .append("topKBeers", 0)
                    .append("topKCoveragePercent", 0.0);
        }
        return res;
    }
}
