package it.unipi.BeerHub.service;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import it.unipi.BeerHub.model.Review;
import it.unipi.BeerHub.model.User;
import it.unipi.BeerHub.repository.*;
import it.unipi.BeerHub.config.mongoDBdriverConfig;
import it.unipi.BeerHub.config.neo4jDriverConfig;
import org.bson.Document;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.*;


import static com.mongodb.client.model.Filters.eq;

@Service
public class userService {
    private final userRepository userRepository;
    MongoCollection<Document> collection;
    Driver graph_driver;
    private final PasswordEncoder passwordEncoder;


    /**
     * service constructor, initializes class objects
     * @param userRepository
     */
    public userService(userRepository userRepository, PasswordEncoder passwordEncoder) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        collection = mongoDBdriverConfig.getCollectionIst("users");
        graph_driver = neo4jDriverConfig.getGraphDriver();
    }

    //CRUD operations

    public List<User> getAll(){
        return userRepository.findAll();
    }

    public User getById(String id){
        Document doc = collection.find(eq("username", id)).first();
        User user = new User();

        user.setUsername(doc.getString("username"));
        user.setName(doc.getString("name"));
        user.setLastname(doc.getString("lastname"));
        user.setEmail(doc.getString("email"));
        user.setGender(doc.getString("gender"));
        user.setAge(doc.getString("age"));
        user.setCity(doc.getString("city"));
        user.setCountry(doc.getString("country"));
        user.setPassword(doc.getString("password"));

        user.setRole(doc.getString("role"));

        user.setReviewIds(doc.getList("reviewIds", String.class));

        return user;
    }

    public Document deleteByUsername(String username) {

        Document result = new Document();
        // Elimina l'utente dalla collezione MongoDB
        long docDel = collection.deleteOne(eq("username", username)).getDeletedCount();
        result.append("docDelCount", docDel);

        Map<String, Object> params = new HashMap<>();
        params.put("username", username);
        String query =
                "MATCH (u:User {username: $username})\n" +
                        "DETACH DELETE u;\n";

        try (var session = graph_driver.session()) {
            try(Transaction tx = session.beginTransaction()) {
                Result res = tx.run(query.toString(), params);
                result.append("graphDelCount", res.consume().counters().nodesDeleted());
                result.append("relDel", res.consume().counters().relationshipsDeleted());
                tx.commit();
            }
        }

        return result;
    }
    /**
     * Registers a new user with BCrypt password hashing and empty review list.
     * @param userData Document containing raw user data from request
     * @return Status message
     */
    public String registerUser(Document userData) {
        String username = userData.getString("username");
        String result = "";

        // 1. Uniqueness check
        if (username == null || username.isEmpty()) return "Error: Username is required.";
        if (collection.countDocuments(eq("username", username)) > 0) {
            return "Error: Username '" + username + "' is already taken.";
        }

        // 2. Password Hashing (BCrypt)
        String rawPassword = userData.getString("password");
        if (rawPassword == null || rawPassword.isEmpty()) return "Error: Password is required.";
        userData.put("password", passwordEncoder.encode(rawPassword));

        userData.append("role", "USER");

        try {
            collection.insertOne(userData);
            result += "User '" + username + "' registered successfully!";
        } catch (Exception e) {
            result += "Error during registration: " + e.getMessage();
        }

        Map<String, Object> params = new HashMap<>();
        params.put("username", username);
        params.put("city", userData.getString("city"));
        params.put("country", userData.getString("country"));
        params.put("email", userData.getString("email"));
        String query =
                "MERGE (u:User {username: $username})\n" +
                "SET  u.city  = $city,\n" +
                "     u.country = $country,\n" +
                "     u.email   = $email";

        try(var session = graph_driver.session()) {
            try(Transaction tx = session.beginTransaction()) {
                Result res = tx.run(query.toString(), params);
                if(res.consume().counters().nodesCreated() == 0) {
                    result += ", User not created on graph db!";
                }else{
                    result += ", User '" + username + "' has been successfully registered on graph db!";
                    tx.commit();
                }
            }catch(Exception e) {
                result += ", User not created on graph db: " + e.getMessage();
            }
        }
        return result;
    }
    /**
     * Promotes a user to ADMIN role by their username.
     * @param username The username of the user to promote.
     * @return true if the user was found and updated, false otherwise.
     */
    public boolean promoteToAdmin(String username) {
        // We look for the user and set the role field to "ADMIN"
        // Using the imported Updates class for cleaner code
        UpdateResult result = collection.updateOne(
                eq("username", username),
                Updates.set("role", "ADMIN")
        );

        // Returns true only if at least one document was actually modified
        return result.getModifiedCount() > 0;
    }

    public boolean followBrewery(String username, String brewery_id) {
        Map<String, Object> params = new HashMap<>();
        params.put("username", username);
        params.put("brewery_id", brewery_id);

        String query =
                "MATCH (u:User {username: $username}), (br:Brewery {brewery_id: $brewery_id}) " +
                        "MERGE (u)-[:FOLLOWS]->(br) " +
                        "RETURN true as success";

        boolean success = false;

        try (var session = graph_driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                Result res = tx.run(query, params);
                if (res.hasNext()) {
                    success = res.next().get("success").asBoolean();
                }
                tx.commit();
            }
        }
        return success;
    }

    public boolean unfollowBrewery(String username, String brewery_id) {
        Map<String, Object> params = new HashMap<>();
        params.put("username", username);
        params.put("brewery_id", brewery_id);

        String query =
                "MATCH (u:User {username: $username})-[r:FOLLOWS]->(br:Brewery {brewery_id: $brewery_id}) " +
                        "DELETE r " +
                        "RETURN true as success";

        boolean success = false;

        try (var session = graph_driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                Result res = tx.run(query, params);
                if (res.hasNext()) {
                    success = res.next().get("success").asBoolean();
                }
                tx.commit();
            }
        }
        return success;
    }

    public Document updateUser(Document in) {
        if (in == null) return null;

        // username è obbligatorio per identificare l'utente, ma NON è aggiornabile
        String username = in.getString("username");
        if (username == null || username.isEmpty()) return null;

        // Recupero utente corrente
        Document current = collection.find(eq("username", username)).first();
        if (current == null) return null;

        // Campi aggiornabili
        Document setFields = new Document();
        if (in.containsKey("name"))     setFields.append("name", in.getString("name"));
        if (in.containsKey("lastname")) setFields.append("lastname", in.getString("lastname"));
        if (in.containsKey("email"))    setFields.append("email", in.getString("email"));
        if (in.containsKey("gender"))   setFields.append("gender", in.getString("gender"));
        if (in.containsKey("age"))      setFields.append("age", in.getString("age"));
        if (in.containsKey("city"))     setFields.append("city", in.getString("city"));
        if (in.containsKey("country"))  setFields.append("country", in.getString("country"));

        // Se non c'è nulla da aggiornare, ritorno lo stato attuale
        if (setFields.isEmpty()) {
            return current;
        }

        // Update su Mongo
        collection.updateOne(eq("username", username), new Document("$set", setFields));

        // Rileggo documento aggiornato (da restituire e per sync Neo4j)
        Document updated = collection.find(eq("username", username)).first();
        if (updated == null) return null;

        // Sync su Neo4j (Neo4j contiene: city, country, email, username)
        Map<String, Object> params = new HashMap<>();
        params.put("username", updated.getString("username"));
        params.put("email", updated.getString("email"));
        params.put("city", updated.getString("city"));
        params.put("country", updated.getString("country"));

        String query =
                "MERGE (u:User {username: $username}) " +
                        "SET u.email = coalesce($email, u.email), " +
                        "    u.city = coalesce($city, u.city), " +
                        "    u.country = coalesce($country, u.country) " +
                        "RETURN true as success";
        //coalesce($email, u.email) evita di settare a null se dal documento manca il campo

        boolean neo4jsuccess = false;
        String neo4jError = null;
        try (var session = graph_driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                Result res = tx.run(query, params);
                if (res.hasNext()) {
                    neo4jsuccess = res.next().get("success").asBoolean();
                } else {
                    neo4jError = "No result returned form Neo4j";
                }
                tx.commit();
            }
        } catch (Exception e) {
            neo4jError = "Neo4j error: " + e.getMessage() ;
        }
        updated.append("success", neo4jsuccess);
        updated.append("neo4jError", neo4jError != null ? neo4jError : "OK");

        return updated;
    }

    //Statistics and complex gets
    public List<User> listByCountry(String country, int limit) {
        List<Document> docs = collection
                .find(eq("country", country))
                .limit(limit)
                .into(new ArrayList<>());

        List<User> out = new ArrayList<>();
        for (Document doc : docs) {
            User u = new User();
            u.setUsername(doc.getString("username"));
            u.setEmail(doc.getString("email"));
            u.setCountry(doc.getString("country"));
            out.add(u);
        }
        return out;
    }

    public List<Document> getSuggestedBeers(String username) {
        Map<String, Object> params = new HashMap<>();
        params.put("username", username);

        String query =
                "MATCH (u:User {username: $username})-[r1:REVIEWED]->(b1:Beer)<-[r2:REVIEWED]-(v:User)\n" +
                        "WHERE v <> u AND r1.score IS NOT NULL AND r2.score IS NOT NULL\n" +
                        "AND abs(r1.score - r2.score) <= 1.0\n" +
                        "WITH u, v, count(b1) AS commonBeers\n" +
                        "WHERE commonBeers >= 1\n" +
                        "MATCH (v)-[r3:REVIEWED]->(suggestedBeer:Beer)\n" +
                        "WHERE r3.score >= 4.0 AND NOT (u)-[:REVIEWED]->(suggestedBeer)\n" +
                        "RETURN suggestedBeer.name AS beerName,\n" +
                        "       suggestedBeer.style AS style,\n" +
                        "       count(DISTINCT v) AS suggestedByXUsers,\n" +
                        "       avg(r3.score) AS avgScoreFromSimilars\n" +
                        "ORDER BY suggestedByXUsers DESC, avgScoreFromSimilars DESC\n" +
                        "LIMIT 5";

        List<Document> recommendations = new ArrayList<>();

        try (var session = graph_driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                Result res = tx.run(query, params);

                while (res.hasNext()) {
                    org.neo4j.driver.Record record = res.next();
                    recommendations.add(
                            new Document()
                                    .append("beerName", record.get("beerName").asString())
                                    .append("style", record.get("style").asString())
                                    .append("suggestedByXUsers", record.get("suggestedByXUsers").asInt())
                                    .append("avgScoreFromSimilars", record.get("avgScoreFromSimilars").asDouble())
                    );
                }
            }
        }
        return recommendations;
    }

    public List<Document> recommendBreweries(String username) {
        Map<String, Object> params = new HashMap<>();
        params.put("username", username);

        String query =
                "MATCH (u:User {username: $username})-[r1:REVIEWED]->(b:Beer)<-[r2:REVIEWED]-(v:User) " +
                        "WHERE v <> u AND r1.score IS NOT NULL AND r2.score IS NOT NULL " +
                        "AND abs(r1.score - r2.score) <= 1.0 " +
                        "WITH u, v, count(b) AS commonBeers " +
                        "WHERE commonBeers >= 1 " +
                        "MATCH (v)-[:FOLLOWS]->(br:Brewery) " +
                        "WHERE NOT (u)-[:FOLLOWS]->(br) " +
                        "RETURN br.brewery_id AS id, " +
                        "       br.name       AS name, " +
                        "       br.city       AS city, " +
                        "       br.country    AS country, " +
                        "       count(DISTINCT v) AS followedBySimUsers " +
                        "ORDER BY followedBySimUsers DESC, name ASC " +
                        "LIMIT 10";

        List<Document> suggestions = new ArrayList<>();

        try (var session = graph_driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                Result res = tx.run(query, params);

                while (res.hasNext()) {
                    org.neo4j.driver.Record record = res.next();
                    suggestions.add(
                            new Document()
                                    .append("id", record.get("id").asString())
                                    .append("name", record.get("name").asString())
                                    .append("city", record.get("city").asString())
                                    .append("country", record.get("country").asString())
                                    .append("followedBySimUsers", record.get("followedBySimUsers").asInt())
                    );
                }
            }
        }
        return suggestions;
    }

    public List<Document> getDrinkingBuddies(String username) {
        Map<String, Object> params = new HashMap<>();
        params.put("username", username);

        String query =
                "MATCH (u:User {username: $username})\n" +
                        "MATCH (v:User)\n" +
                        "WHERE v <> u AND v.city = u.city AND v.country = u.country\n" +
                        "MATCH (u)-[r1:REVIEWED]->(b:Beer)<-[r2:REVIEWED]-(v)\n" +
                        "WHERE r1.score IS NOT NULL AND r2.score IS NOT NULL\n" +
                        "AND abs(r1.score - r2.score) <= 1.0\n" +
                        "WITH v, count(b) AS commonBeers\n" +
                        "WHERE commonBeers >= 1\n" +
                        "RETURN v.username AS username,\n" +
                        "       v.city     AS city,\n" +
                        "       v.email    AS email,\n" +
                        "       commonBeers\n" +
                        "ORDER BY commonBeers DESC, username ASC\n" +
                        "LIMIT 5";

        List<Document> buddies = new ArrayList<>();

        try (var session = graph_driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                Result res = tx.run(query, params);

                while (res.hasNext()) {
                    org.neo4j.driver.Record record = res.next();
                    buddies.add(
                            new Document()
                                    .append("username", record.get("username").asString())
                                    .append("email", record.get("email").asString())
                                    .append("city", record.get("city").asString())
                                    .append("commonBeers", record.get("commonBeers").asInt())
                    );
                }
            }
        }
        return buddies;
    }
}