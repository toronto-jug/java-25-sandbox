package ca.tjug;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public record WeatherDataPoint(String csv) {

}
