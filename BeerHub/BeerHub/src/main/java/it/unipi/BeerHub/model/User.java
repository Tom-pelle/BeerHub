package it.unipi.BeerHub.model;

import lombok.*;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
@Document(collection = "users")
public class User {
    private String username; //univoco, si può usare come chiave nella ricerca su mongo

    private String name;
    private String lastname;
    private String email;
    private String gender;
    private String age;
    private String city;
    private String country;
    private String password; //hash
    private String role;

    private List<String> reviewIds;
}
