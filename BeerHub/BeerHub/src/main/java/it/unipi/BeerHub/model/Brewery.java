package it.unipi.BeerHub.model;

import lombok.*;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
@Document(collection = "breweries")
public class Brewery {
    @Id
    private String id;

    private String brewery_id;

    private String brewery_name;
    private String brewery_city;
    private String brewery_country;
    private String brewery_type;

    private List<org.bson.Document> featuredBeers;
}
