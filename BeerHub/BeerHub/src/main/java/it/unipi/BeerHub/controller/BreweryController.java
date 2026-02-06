package it.unipi.BeerHub.controller;

import com.mongodb.client.result.UpdateResult;
import it.unipi.BeerHub.model.Beer;
import it.unipi.BeerHub.model.Brewery;
import it.unipi.BeerHub.service.breweryService;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/brewery")
@RequiredArgsConstructor
public class BreweryController {
    private final breweryService breweryService;

    @GetMapping("/get/by-id")
    public Optional<Brewery>  getBrewery(@RequestParam (required = true) String id){return breweryService.findById(id);}

    @GetMapping("/get/browsing-list")
    public List<Brewery> FilteredGet(
            @RequestParam(required = false) String name,
            @RequestParam(required = false) String country
    )
    {
        return breweryService.FilteredGet(name, country);
    }

    @GetMapping("/stats/abv-profile-cities")
    public List<Document> getTopCitiesAbvProfileByCountry(@RequestParam (required = true) String country) {
        return breweryService.getTopCitiesAbvProfileByCountry(country);
    }

    // --- ENDPOINTS ADMIN ---

    // Update by ID
    @PutMapping("/admin/update/{id}")
    public Document updateBrewery(@PathVariable String id, @RequestBody Document updateData) {
        return breweryService.updateBrewery(id, updateData);
    }

    // Update by Name
    @PutMapping("/admin/update-by-name")
    public Document updateByBreweryName(@RequestParam String name, @RequestBody Document updateData) {
        Brewery b = breweryService.getByName(name);
        if (b == null) return new Document().append("brewery ","not exists");
        return breweryService.updateBrewery(b.getBrewery_id(), updateData);
    }

    // Add beer to the catalog by brewery NAME
    @PostMapping("/admin/add-beer-by-name")
    public Document addBeerByBreweryName(@RequestParam String brewery_id, @RequestBody Document beerData) {
        return breweryService.addBeerToBrewery(brewery_id, beerData);
    }

    // Admin only: Delete a brewery using its name instead of ID
    @DeleteMapping("/admin/delete-by-id")
    public Document deleteByBreweryId(@RequestParam String id) {
        return breweryService.deleteByBreweryId(id);
    }

    // Admin only: Create a new brewery
    @PostMapping("/admin/insert")
    public String insertBrewery(@RequestBody Document breweryData) {
        try {
            breweryService.insertBrewery(breweryData);
            return "Brewery '" + breweryData.getString("brewery_name") + "' inserted successfully!";
        } catch (Exception e) {
            return "Error during insertion: " + e.getMessage();
        }
    }
}
