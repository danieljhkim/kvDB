package com.danieljhkim.kvdb.kvadmin.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.ConstructorDetector;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

/**
 * Jackson JSON configuration for REST API serialization.
 *
 * <p>Unknown properties fail deserialization. Typed mutation bodies therefore reject
 * misspelled fields instead of ignoring them. Open {@code Map} bodies are unchanged.
 */
@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper(Jackson2ObjectMapperBuilder builder) {
        ObjectMapper mapper = builder.modules(new JavaTimeModule())
                .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .featuresToEnable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
        // Boot's builder customizer disables this feature unless the matching property is set.
        // Enable it on the instance so a customizer cannot leave typos as silent no-ops.
        mapper.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        // A visible single-string constructor is an implicit creator, so a JSON string was
        // stored as the leader id or status. Require @JsonCreator for that path. Fields and
        // setters still bind JSON objects.
        mapper.setConstructorDetector(ConstructorDetector.DEFAULT.withRequireAnnotation(true));
        return mapper;
    }
}
