package it.unipi.BeerHub.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;

import static org.springframework.security.config.Customizer.withDefaults;

@Configuration
@EnableWebSecurity
public class securityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .csrf(csrf -> csrf.disable())
                .authorizeHttpRequests(auth -> auth
                        // 1. ADMIN AREA: Exclusive access to management operations
                        .requestMatchers("/api/user/admin/**").hasRole("ADMIN")
                        .requestMatchers("/api/beer/admin/**").hasRole("ADMIN")
                        .requestMatchers("/api/brewery/admin/**").hasRole("ADMIN")
                        .requestMatchers("/api/review/admin/**").hasRole("ADMIN")

                        // 2. STATISTICS & AGGREGATIONS: Open to both USERS and ADMINS
                        .requestMatchers("/api/beer/stats/**").hasAnyRole("USER", "ADMIN")
                        .requestMatchers("/api/brewery/stats/**").hasAnyRole("USER", "ADMIN")
                        .requestMatchers("/api/review/stats/**").hasAnyRole("USER", "ADMIN")
                        .requestMatchers("/api/beer/trends").hasAnyRole("USER", "ADMIN")
                        .requestMatchers("/api/user/get/list").hasAnyRole("USER", "ADMIN")
                        .requestMatchers("/api/user/get/suggestedBeers").hasAnyRole("USER", "ADMIN")
                        .requestMatchers("/api/user/get/suggestedBreweries").hasAnyRole("USER", "ADMIN")

                        // 3. USER OPERATIONS: Reviews (USER + ADMIN for security)
                        .requestMatchers("/api/review/insert/**").hasAnyRole("USER", "ADMIN")
                        .requestMatchers("/api/user/post/follow").hasAnyRole("USER", "ADMIN")
                        .requestMatchers("/api/user/delete/unfollow").hasAnyRole("USER", "ADMIN")

                        // 4. PUBLIC: Read-only methods only (GET)
                        .requestMatchers("/api/beer/get/**").permitAll()
                        .requestMatchers("/api/brewery/get/**").permitAll()
                        .requestMatchers("/api/user/get/**").permitAll()
                        .requestMatchers("/api/review/get/**").permitAll()

                        .requestMatchers("/api/user/register").not().hasAnyRole("USER", "ADMIN")

                        // Swagger always open
                        .requestMatchers("/swagger-ui/**", "/v3/api-docs/**", "/swagger-ui.html").permitAll()

                        .anyRequest().authenticated()
                )
                .httpBasic(withDefaults());

        return http.build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        // Utilizza BCrypt per confrontare le password nel DB con quelle inserite nel login
        return new BCryptPasswordEncoder();
    }
}