package ca.tjug;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Record representing a single weather data point from Environment Canada.
 * Fields are based on common weather data metrics.
 */
@JsonPropertyOrder({
    "Date/Time", "Year", "Month", "Day", "Time", "Temp (°C)", "Dew Point Temp (°C)",
    "Rel Hum (%)", "Wind Dir (10s deg)", "Wind Spd (km/h)", "Visibility (km)",
    "Stn Press (kPa)", "Weather"
})
public record WeatherDataPoint(
    @JsonProperty("Date/Time") String dateTime,
    @JsonProperty("Year") Integer year,
    @JsonProperty("Month") Integer month,
    @JsonProperty("Day") Integer day,
    @JsonProperty("Time") String time,
    @JsonProperty("Temp (°C)") Double temperature,
    @JsonProperty("Dew Point Temp (°C)") Double dewPoint,
    @JsonProperty("Rel Hum (%)") Integer relativeHumidity,
    @JsonProperty("Wind Dir (10s deg)") Integer windDirection,
    @JsonProperty("Wind Spd (km/h)") Double windSpeed,
    @JsonProperty("Visibility (km)") Double visibility,
    @JsonProperty("Stn Press (kPa)") Double stationPressure,
    @JsonProperty("Weather") String weather
) {
    /**
     * Parses the date and time fields into a LocalDateTime object.
     * 
     * @return LocalDateTime representing the date and time of this weather data point
     */
    public LocalDateTime getDateTime() {
        if (dateTime != null && !dateTime.isEmpty()) {
            try {
                return LocalDateTime.parse(dateTime, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            } catch (Exception e) {
                // If direct parsing fails, try to construct from individual components
                if (year != null && month != null && day != null && time != null) {
                    String dateTimeStr = String.format("%04d-%02d-%02dT%s", year, month, day, time);
                    try {
                        return LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                    } catch (Exception ex) {
                        // If all parsing attempts fail, return null
                        return null;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Constructor that takes a raw CSV line (for backward compatibility).
     * This should not be used for new code.
     * 
     * @param csv the raw CSV line
     * @deprecated Use Jackson CSV parsing instead
     */
    @Deprecated
    public WeatherDataPoint(String csv) {
        this(csv, null, null, null, null, null, null, null, null, null, null, null, null);
    }
}
