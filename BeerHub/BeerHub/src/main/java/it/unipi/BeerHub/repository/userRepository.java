package it.unipi.BeerHub.repository;

import it.unipi.BeerHub.model.User;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface userRepository extends MongoRepository<User, String>{
}