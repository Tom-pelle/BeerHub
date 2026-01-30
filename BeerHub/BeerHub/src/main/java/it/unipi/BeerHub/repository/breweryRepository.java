package it.unipi.BeerHub.repository;

import it.unipi.BeerHub.model.Brewery;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface breweryRepository extends MongoRepository<Brewery, String> {
}