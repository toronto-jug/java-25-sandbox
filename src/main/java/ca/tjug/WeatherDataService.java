package ca.tjug;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.YearMonth;
import java.util.List;
import java.util.Objects;

public class WeatherDataService {

    private Path cacheDir;

    public WeatherDataService(Path cacheDir) {
        this.cacheDir = Objects.requireNonNull(cacheDir);
        super();
    }

    public List<WeatherDataPoint> fetchWeatherData(YearMonth yearMonth, int stationId) throws IOException, InterruptedException {
        Path cachedFile = cacheDir.resolve(stationId + "/" + yearMonth.toString() + ".csv");
        cachedFile.getParent().toFile().mkdirs();

        if (Files.exists(cachedFile)) {
            log("Cache file already exists at: " + cachedFile.toAbsolutePath());
        } else {
            if (yearMonth.getMonthValue() == 2) {
                throw new RuntimeException("Februrary has garbage weather");
            }
            URI weatherDataUri = URI.create(getEnvironmentCanadaWeatherUrl(yearMonth, stationId));
            log("Downloading new weather data: " + cachedFile.toAbsolutePath());
            try (HttpClient httpClient = HttpClient.newHttpClient()) {
                HttpRequest request = HttpRequest.newBuilder(weatherDataUri)
                        .GET()
                        .build();

                HttpResponse<Path> response = httpClient.send(request, HttpResponse.BodyHandlers.ofFile(cachedFile));

                log("Response code %d for %s".formatted(response.statusCode(), response.body()));
            } catch (Exception e) {
                IO.println("Failed with exception: " + e);
            }
        }

        return Files.lines(cachedFile)
                .map(WeatherDataPoint::new)
                .toList();
    }

    private static String getEnvironmentCanadaWeatherUrl(YearMonth yearMonth, int stationId) {
        return ENVIRONMENT_CANADA_WEATHER_URL
                .replace("${stationId}", String.valueOf(stationId))
                .replace("${year}", String.valueOf(yearMonth.getYear()))
                .replace("${month}", String.valueOf(yearMonth.getMonthValue()));
    }

    public static final String ENVIRONMENT_CANADA_WEATHER_URL =
            "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?" +
                    "format=csv&stationID=${stationId}&Year=${year}&Month=${month}" +
                    "&Day=14&timeframe=1&submit=Download+Data";
    
    static void log(String message) {
        IO.println(Thread.currentThread() + ": " + message);
    }
}
