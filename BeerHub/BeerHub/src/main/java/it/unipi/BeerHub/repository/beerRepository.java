package it.unipi.BeerHub.repository;

import it.unipi.BeerHub.model.Beer;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface beerRepository extends MongoRepository<Beer, String>{

}
