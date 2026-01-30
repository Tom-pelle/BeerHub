package it.unipi.BeerHub.controller;


import com.mongodb.client.result.UpdateResult;
import it.unipi.BeerHub.model.Beer;
import it.unipi.BeerHub.service.*;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/beer")
@RequiredArgsConstructor
public class BeerController {
    private final beerService beerService;

    @GetMapping("/get/by-id")
    public Optional<Beer> getBeer(@RequestParam (required = true) String id){return beerService.findById(id);}

    /**
     * Endpoint per le richieste provenienti dalla barra di ricerca
     */
    @GetMapping("/get/browsing-list")
    public List<Beer> FilteredGet(
            @RequestParam(required = false) String name,
            @RequestParam(required = false) String style,
            @RequestParam(required = false) String country
    )
    {
        return beerService.FilteredGet(name, style, country);
    }

    @GetMapping("/stats/avg-score")
    public Document computeAvgScore(@RequestParam(required = true)String id){return beerService.AvgScore(id);}

    @GetMapping("/stats/country-style-fingerprint")
    public Document countryStyleFingerprint(
            @RequestParam(required = true) String country,
            @RequestParam(required = false, defaultValue = "10") int k
    ) {
        return beerService.CountryBeerStyles(country, k);
    }

    @GetMapping("/stats/trends")
    public List<Document> getTrends(  // List<Document> !
            @RequestParam(required = false) String country,
            @RequestParam(required = false) String style,
            @RequestParam(defaultValue = "true") boolean positiveTrend,
            @RequestParam(defaultValue = "5") double minTrend
    ) {
        return beerService.getTrends(country, style, positiveTrend, minTrend);
    }

    // Admin only: Update beer by Name
    @PutMapping("/admin/update-by-name")
    public Document updateByBeerName(@RequestParam String name, @RequestBody Document updateData) {
        Beer b = beerService.getByName(name);
        if (b == null) return null;

        return beerService.updateBeer(b.getBeer_id(), updateData);
    }

    // Admin only: Update beer by ID
    @PutMapping("/admin/update/{id}")
    public Document updateBeer(@PathVariable String id, @RequestBody Document updateData) {
        return beerService.updateBeer(id, updateData);
    }

    // Admin only: Delete a beer by its name
    @DeleteMapping("/admin/delete-by-name")
    public Document deleteByBeerName(@RequestParam String name) {
        Document delete = beerService.deleteByBeerName(name);

        return delete;
    }

    // Admin only: Insert a new beer

    @PostMapping("/admin/insert")
    public String insertBeer(@RequestBody Document beerData) {
        try {
            return beerService.insertBeer(beerData);
        } catch (Exception e) {
            return "Critical error during insertion: " + e.getMessage();
        }
    }
}
