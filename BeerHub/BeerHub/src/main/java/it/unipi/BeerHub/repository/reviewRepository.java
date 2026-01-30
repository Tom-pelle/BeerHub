package it.unipi.BeerHub.repository;

import it.unipi.BeerHub.model.Review;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface reviewRepository extends MongoRepository<Review, String>{
}