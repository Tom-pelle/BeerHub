package it.unipi.BeerHub.service;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import it.unipi.BeerHub.config.mongoDBdriverConfig;
import it.unipi.BeerHub.config.neo4jDriverConfig;
import it.unipi.BeerHub.model.Beer;
import it.unipi.BeerHub.model.Brewery;
import it.unipi.BeerHub.repository.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;

@Service
public class breweryService {
    private final breweryRepository breweryRepository;
    MongoCollection<Document> collection;
    MongoCollection<Document> collectionBeers;
    Driver graph_driver;

    /**
     * service constructor, initializes class objects
     * @param breweryRepository
     */
    public breweryService(breweryRepository breweryRepository){
        this.breweryRepository = breweryRepository;
        collection = mongoDBdriverConfig.getCollectionIst("breweries");
        collectionBeers = mongoDBdriverConfig.getCollectionIst("beers");
        graph_driver = neo4jDriverConfig.getGraphDriver();
    }

    //CRUD operations
    public List<Brewery> getAll(){
        return breweryRepository.findAll();
    }

    public Brewery getById(String id){
        Document doc = collection.find(eq("brewery_id", id)).first();
        Brewery br = new Brewery();

        br.setBrewery_id(doc.getString("brewery_id"));
        br.setBrewery_name(doc.getString("brewery_name"));
        br.setBrewery_city(doc.getString("brewery_city"));
        br.setBrewery_country(doc.getString("brewery_country"));
        br.setBrewery_type(doc.getString("brewery_type"));
        br.setFeaturedBeers(doc.getList("featuredBeers", Document.class));

        return br;
    }

    public Optional<Brewery> findById(String id){return breweryRepository.findById(id);}


    public Brewery getByName(String name) {
        Document doc = collection.find(eq("brewery_name", name)).first();
        if (doc == null) return null;

        Brewery br = new Brewery();
        br.setBrewery_id(doc.getString("brewery_id"));
        br.setBrewery_name(doc.getString("brewery_name"));
        return br;
    }

    // --- ADMIN METHODS ---
    public Document updateBrewery(String breweryId, Document updateData) {
        // 1. Fetch the current brewery document to identify the current (old) name
        Document existingBrewery = collection.find(eq("brewery_id", breweryId)).first();
        if (existingBrewery == null) return null;

        Document result = new Document();

        String oldName =  existingBrewery.getString("brewery_name");
        String newName =  updateData.getString("brewery_name");

        List<Bson> updates = new ArrayList<>();
        if (updateData.containsKey("brewery_name")) updates.add(Updates.set("brewery_name", updateData.getString("brewery_name")));
        if (updateData.containsKey("brewery_city")) updates.add(Updates.set("brewery_city", updateData.getString("brewery_city")));
        if (updateData.containsKey("brewery_country")) updates.add(Updates.set("brewery_country", updateData.getString("brewery_country")));
        if (updateData.containsKey("brewery_type")) updates.add(Updates.set("brewery_type", updateData.getString("brewery_type")));

        if (updates.isEmpty()) return null;

        // 2. Perform the update on the brewery document
        UpdateResult docres = collection.updateOne(eq("brewery_id", breweryId), Updates.combine(updates));
        result.put("docMatched", docres.getMatchedCount());
        result.put("docModified", docres.getModifiedCount());


        if (docres.getModifiedCount() > 0 && !newName.equals(oldName)) {

            Document feat = collection.aggregate(List.of(
                    match(eq("brewery_id", breweryId)),
                    project(fields(include(
                            "brewery_id", "featuredBeers.beer_id"
                    )))
            )).first();

            if(!feat.containsKey("featuredBeers")){return result;}
            List<String> ids = new ArrayList<>();
            feat.getList("featuredBeers", Document.class).forEach(d -> {
                ids.add(d.getString("beer_id"));
            });

            UpdateResult beerRes = collectionBeers.updateMany(
                    in("beer_id", ids),
                    Updates.set("brewery_name", newName)
            );
            result.put("beerMatched", beerRes.getMatchedCount());
            result.put("beerModified", beerRes.getModifiedCount());
        }

        //GRAFO
        StringBuilder query = new StringBuilder(
                "MATCH (b:Brewery {brewery_id: $brewery_id}) "
        );

        Map<String, Object> params = new HashMap<>();
        List<String> sets = new ArrayList<>();
        params.put("brewery_id", breweryId);
        if((updateData.containsKey("brewery_name"))){
            params.put("brewery_name", updateData.getString("brewery_name"));
            sets.add("b.name = $brewery_name");
        }
        if((updateData.containsKey("brewery_city"))){
            params.put("brewery_city", updateData.getString("brewery_city"));
            sets.add("b.city = $brewery_city");
        }
        if((updateData.containsKey("brewery_country"))){
            params.put("brewery_country", updateData.getString("brewery_country"));
            sets.add("b.country = $brewery_country");
        }
        Result res = null;
        try (var session = graph_driver.session()) {
            query.append("SET ").append(String.join(", ", sets));
            System.out.println(query.toString());
            System.out.println(params);
            try(Transaction tx = session.beginTransaction()) {
                res = tx.run(query.toString(), params);
                tx.commit();
                result.put("graphResult", res.consume().counters().propertiesSet());
            }
        }

        return result;
    }

    /**
     * Adds a specific beer document to the brewery's featuredBeers list.
     */
    public Document addBeerToBrewery(String breweryId, Document beerData) {
        UpdateResult result = collection.updateOne(eq("brewery_id", breweryId), Updates.push("featuredBeers", beerData));

        Map<String, Object> param = new HashMap<>();
        param.put("beer_id",  beerData.getString("beer_id"));
        param.put("brewery_id",  breweryId);
        String query1 =
                "MATCH (B:Beer {beer_id: $beer_id}) return B";

        String query2 =
                "MATCH (b:Beer {beer_id: $beer_id})\n" +
                "MATCH (br:Brewery {brewery_id: $brewery_id})\n" +
                "MERGE (b)-[r:PRODUCED_BY]->(br)";

        Document toReturn = new Document()
                .append("docMatched" , result.getMatchedCount())
                .append("docModified" , result.getModifiedCount());
        try(var session = graph_driver.session()) {
            try(Transaction tx = session.beginTransaction()) {
                Result res = tx.run(query1, param);
                if(res.hasNext()){
                    res = tx.run(query2, param);
                    tx.commit();
                    toReturn.append("graphRes", res.consume().counters().relationshipsCreated());
                }else{
                    toReturn.append("graphResult", "beer not found");
                }
            }
        }
        return toReturn;
    }

    // Method to delete a brewery by its name
    public Document deleteByBreweryId(String id) {
        // First, find the brewery to get its ID or confirm existence

        // Execute the deletion using the retrieved brewery_id
        Document result = new Document();
        result.append("deletedCountDoc", collection.deleteOne(eq("brewery_id", id)).getDeletedCount());

        Map<String, Object> param = new HashMap<>();
        param.put("brewery_id", id);
        String query =
                "MATCH (br:Brewery {brewery_id: $brewery_id})\n" +
                "DETACH DELETE br;";

        try(var session = graph_driver.session()) {
            try(Transaction tx = session.beginTransaction()) {
                Result res = tx.run(query, param);
                result.append("graphRes", res.consume().counters().nodesDeleted() + res.consume().counters().relationshipsDeleted());
                tx.commit();
            }
        }

        return result;
    }

    // Method to insert a new brewery document
    public void insertBrewery(Document breweryData) {
        // Check if featuredBeers exists, otherwise initialize as empty list
        if (!breweryData.containsKey("featuredBeers")) {
            breweryData.append("featuredBeers", new ArrayList<Document>());
        }
        collection.insertOne(breweryData);

        Map<String, Object> params = new HashMap<>();
        params.put("brewery_id", breweryData.getString("brewery_id"));
        params.put("city", breweryData.getString("brewery_city"));
        params.put("country", breweryData.getString("brewery_country"));
        params.put("name", breweryData.getString("brewery_name"));

        String query1 =
                "MERGE (br:Brewery {brewery_id: $brewery_id})\n" +
                "SET br.name = $name,\n" +
                "    br.country = $country,\n" +
                "    br.city = $city";

        try(var session = graph_driver.session()) {
            try(Transaction tx = session.beginTransaction()) {
                tx.run(query1, params);

                if (breweryData.containsKey("featuredBeers")){
                    params.put("featuredBeers", breweryData.get("featuredBeers"));
                    String query2 =
                            "MATCH (br:Brewery {brewery_id: $brewery_id})\n" +
                            "WITH br, coalesce($featuredBeers, []) AS beers\n" +
                            "UNWIND beers AS beer\n" +
                            "MATCH (b:Beer {beer_id: beer.beer_id})\n" +
                            "MERGE (b)-[:PRODUCED_BY]->(br)";
                    tx.run(query2, params);
                }
                tx.commit();
            }
        }
    }

    //Statistic functionst with Mongo aggregations
    public List<Brewery> FilteredGet(String name, String country) {
        List<Bson> pipeline = new ArrayList<>();
        if(name != null){
            pipeline.add(match(text(name)));
            pipeline.add(
                    project(fields(
                            include("_id", "brewery_name", "brewery_country", "brewery_city", "brewery_type", "featuredBeers"),
                            metaTextScore("score")
                    ))
            );
            pipeline.add(match(regex("brewery_name", Pattern.compile(name, Pattern.CASE_INSENSITIVE))));
        }
        if(country != null){pipeline.add(match(regex("brewery_country", Pattern.compile(country, Pattern.CASE_INSENSITIVE))));}
        if(name!=null){pipeline.add(sort(orderBy(metaTextScore("score"), ascending("_id"))));}
        else{pipeline.add(sort(ascending("_id")));}
        pipeline.add(limit(50));

        List<Document> results = collection.aggregate(pipeline).into(new ArrayList<>());

        List<Brewery> brs = new ArrayList<>();

        for(Document d : results){
            Brewery b = new Brewery();
            b.setId(d.getObjectId("_id").toHexString());
            b.setBrewery_name(d.getString("brewery_name"));
            b.setBrewery_city(d.getString("brewery_city"));
            b.setBrewery_country(d.getString("brewery_country"));
            b.setBrewery_type(d.getString("brewery_type"));
            b.setFeaturedBeers(d.getList("featuredBeers", Document.class));
            //b.setFeatured_beers(null);
            brs.add(b);
        }
        return brs;
    }

    public List<Document> getTopCitiesAbvProfileByCountry(String country) {
        List<Bson> pipeline = new ArrayList<>();

        // 1) Solo birrifici del paese richiesto con almeno 1 featured beer
        pipeline.add(match(and(
                eq("brewery_country", country),
                exists("featuredBeers.0", true)
        )));

        // 2) Profilo ABV del singolo birrificio
        pipeline.add(project(fields(
                include("brewery_city"),
                computed("avgAbvBrewery", new Document("$avg", "$featuredBeers.abv")),
                computed("minAbvBrewery", new Document("$min", "$featuredBeers.abv")),
                computed("maxAbvBrewery", new Document("$max", "$featuredBeers.abv"))
        )));

        // 3) Scarta birrifici senza ABV valido
        pipeline.add(match(ne("avgAbvBrewery", null)));

        // 4) Aggregazione per città
        pipeline.add(group("$brewery_city",
                Accumulators.sum("breweriesCount", 1),
                Accumulators.avg("avgAbvCity", "$avgAbvBrewery"),
                Accumulators.avg("avgMinAbvCity", "$minAbvBrewery"),
                Accumulators.avg("avgMaxAbvCity", "$maxAbvBrewery")
        ));

        // 5) Solo città con almeno 5 birrifici
        pipeline.add(match(gte("breweriesCount", 5)));

        // 6) Ordinamento e Top 10
        pipeline.add(sort(orderBy(
                descending("avgAbvCity"),
                descending("breweriesCount")
        )));
        pipeline.add(limit(10));

        pipeline.add(project(fields(
                excludeId(),
                computed("city", "$_id"),
                include("breweriesCount"),
                computed("avgAbvCity", new Document("$round", Arrays.asList("$avgAbvCity", 2))),
                computed("avgMinAbvCity", new Document("$round", Arrays.asList("$avgMinAbvCity", 2))),
                computed("avgMaxAbvCity", new Document("$round", Arrays.asList("$avgMaxAbvCity", 2)))
        )));

        return collection.aggregate(pipeline).into(new ArrayList<>());
    }

}