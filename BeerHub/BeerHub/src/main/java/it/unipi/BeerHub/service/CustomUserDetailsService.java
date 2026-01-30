package it.unipi.BeerHub.service;

import it.unipi.BeerHub.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

@Service
public class CustomUserDetailsService implements UserDetailsService {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        // Search for the user in the "users" collection
        Query query = new Query(Criteria.where("username").is(username));
        User user = mongoTemplate.findOne(query, User.class);

        if (user == null) {
            throw new UsernameNotFoundException("User not found: " + username);
        }

        // Preliminary role validation
        if (user.getRole() == null || user.getRole().isEmpty()) {
            System.out.println("WARNING: The 'role' field is NULL or EMPTY for user: " + username);
        }

        // Return the UserDetails object with the assigned role (e.g., ADMIN or USER)
        return org.springframework.security.core.userdetails.User.builder()
                .username(user.getUsername())
                .password(user.getPassword())
                .roles(user.getRole())
                .build();
    }
}