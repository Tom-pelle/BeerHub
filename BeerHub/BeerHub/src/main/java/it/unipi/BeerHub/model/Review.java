package it.unipi.BeerHub.model;

import lombok.*;
import org.springframework.data.mongodb.core.mapping.Document;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
@Document(collection = "reviews")
public class Review {
    private String review_id;

    private String username;
    private String date;
    private String text;
    private Double score;
    private String beer_name;
}
