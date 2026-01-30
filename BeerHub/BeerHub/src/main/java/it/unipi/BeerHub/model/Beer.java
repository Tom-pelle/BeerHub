package it.unipi.BeerHub.model;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
@Document(collection = "beers")
public class Beer {
    @Id
    private String id;
    private String beer_id;   // userai "beer_id" come chiave

    private String name;
    private String style;
    private Double abv;
    private String country;
    private String brewery_name;

    private List<Review> latestReviews;
    private List<String> otherReviewIDs;
}
