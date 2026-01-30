package it.unipi.BeerHub.controller;

import it.unipi.BeerHub.model.Review;
import it.unipi.BeerHub.service.reviewService;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/review")
@RequiredArgsConstructor
public class ReviewController {
    private final reviewService reviewService;

    @GetMapping("/get/by-ids")
    public List<Review> getReviews(@RequestBody List<String> ids){return reviewService.findByIds(ids);}

    @PostMapping("/insert/insertReview")
    public Document insertReview(@RequestBody Document review, Authentication authentication){
        String loggedUser = authentication.getName();

        review.put("username", loggedUser);
        return reviewService.insertReview(review);
    }

    @GetMapping("/stats/top-active-users")
    public List<Document> getTop15ActiveUsersStats() {
        return reviewService.getTop15ActiveUsersStats();
    }

    @DeleteMapping("admin/delete-by-id")
    public Document deleteReview(@RequestParam String review_id){return reviewService.deleteReview(review_id);}

}
