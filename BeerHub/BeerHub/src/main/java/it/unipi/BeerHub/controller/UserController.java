package it.unipi.BeerHub.controller;

import java.util.ArrayList;
import java.util.List;

import it.unipi.BeerHub.model.User;
import it.unipi.BeerHub.service.userService;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.bson.Document;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/user")
@RequiredArgsConstructor
public class UserController {

    private final userService userService;

    @GetMapping("/get/by-usr")
    public ResponseEntity<UserPublic> getUserPublic(@RequestParam (required = true) String username) {
        User u = userService.getById(username); // cerca per username
        if (u == null) return ResponseEntity.notFound().build();

        UserPublic dto = new UserPublic(
                u.getUsername(),
                u.getEmail(),
                u.getCountry()
        );
        return ResponseEntity.ok(dto);
    }

    @GetMapping("/get/list")
    public ResponseEntity<List<UserPublic>> listUsersByCountry(@RequestParam (required = true) String country) {
        List<User> users = userService.listByCountry(country, 10);

        List<UserPublic> out = users.stream()
                .map(u -> new UserPublic(u.getUsername(), u.getEmail(), u.getCountry()))
                .toList();

        return ResponseEntity.ok(out);
    }

    @DeleteMapping("/admin/delete")
    public Document deleteUser(@RequestParam String username) {
        return userService.deleteByUsername(username);
    }

    @GetMapping("/get/suggestedBeers")
    public ResponseEntity<List<BeerRecommended>> getSuggestedBeers(Authentication auth) {
        String username = auth.getName();
        List<Document> list = userService.getSuggestedBeers(username);

        List<BeerRecommended> out = new ArrayList<>();
        list.forEach(d -> {
            out.add(new BeerRecommended(
                    d.getString("beerName"),
                    d.getString("style"),
                    d.getInteger("suggestedByXUsers"),
                    d.getDouble("avgScoreFromSimilars")
            ));
        });

        return ResponseEntity.ok(out);
    }

    @GetMapping("/get/suggestedBreweries")
    public ResponseEntity<List<BrewerySuggested>> getSuggestedBreweries(Authentication auth) {
        String username = auth.getName();
        List<Document> list = userService.recommendBreweries(username);

        List<BrewerySuggested> out = new ArrayList<>();
        list.forEach(d -> {
            out.add(new BrewerySuggested(
                    d.getString("id"),
                    d.getString("name"),
                    d.getString("city"),
                    d.getString("country"),
                    d.getInteger("followedBySimUsers")
            ));
        });

        return ResponseEntity.ok(out);
    }


    @PostMapping("/register")
    public ResponseEntity<String> registerUser(@RequestBody Document userData) {
        String result = userService.registerUser(userData);
        if (result.startsWith("Error")) {
            return ResponseEntity.badRequest().body(result);
        }
        return ResponseEntity.ok(result);
    }

    // Admin only: Promote a user to ADMIN role
    @PutMapping("/admin/promote")
    public ResponseEntity<String> promoteUser(@RequestParam String username) {
        boolean success = userService.promoteToAdmin(username);

        if (success) {
            return ResponseEntity.ok("User '" + username + "' has been successfully promoted to ADMIN.");
        } else {
            // This happens if the user doesn't exist or is already an ADMIN
            return ResponseEntity.status(404).body("Error: User not found or already has ADMIN role.");
        }
    }

    @GetMapping("/get/drinkingBuddies")
    public ResponseEntity<List<DrinkingBuddy>> getDrinkingBuddies(Authentication auth) {
        String username = auth.getName();
        List<Document> list = userService.getDrinkingBuddies(username);

        List<DrinkingBuddy> out = new ArrayList<>();
        list.forEach(d -> {
            out.add(new DrinkingBuddy(
                    d.getString("username"),
                    d.getString("email"),
                    d.getString("city"),
                    d.getInteger("commonBeers")
            ));
        });

        return ResponseEntity.ok(out);
    }

    @PostMapping("/post/follow")
    public ResponseEntity<String> followBrewery(
            @RequestParam String breweryName,
            Authentication auth) {
        String username = auth.getName();
        boolean followed = userService.followBrewery(username, breweryName);
        if (followed) {
            return ResponseEntity.ok("Successfully followed brewery: " + breweryName);
        } else {
            return ResponseEntity.badRequest().body("User or brewery not found");
        }
    }

    @DeleteMapping("/delete/unfollow")
    public ResponseEntity<String> unfollowBrewery(
            @RequestParam String breweryName,
            Authentication auth) {
        String username = auth.getName();
        boolean unfollowed = userService.unfollowBrewery(username, breweryName);
        if (unfollowed) {
            return ResponseEntity.ok("Successfully unfollowed brewery: " + breweryName);
        } else {
            return ResponseEntity.badRequest().body("Follow relationship not found");
        }
    }

    @PutMapping("/put/update")
    public ResponseEntity<Document> updateUser(@RequestBody Document userDoc, Authentication auth) {
        userDoc.put("username", auth.getName());
        Document updated = userService.updateUser(userDoc);
        if (updated == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(updated);
    }

    @Getter
    @Setter
    @AllArgsConstructor
    static class UserPublic {
        private String username;
        private String email;
        private String country;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    static class BrewerySuggested {
        private String id;
        private String name;
        private String city;
        private String country;
        private int followedBySimUsers;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    static class DrinkingBuddy {
        private String username;
        private String email;
        private String city;
        private int commonBeers;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    static class BeerRecommended {
        private String beerName;
        private String style;
        private int suggestedByXUsers;
        private double avgScoreFromSimilars;
    }
}