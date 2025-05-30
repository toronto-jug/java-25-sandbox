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

    /**
     * Fetches weather data for a specific year-month and station ID.
     * If the data is already cached, it will be loaded from the cache.
     * Otherwise, it will be downloaded from Environment Canada's website.
     *
     * @param yearMonth the year and month to fetch data for
     * @param stationId the station ID to fetch data for
     * @return a list of WeatherDataPoint objects
     * @throws IOException if there's an error reading or writing files
     * @throws InterruptedException if the HTTP request is interrupted
     */
    public List<WeatherDataPoint> fetchWeatherData(YearMonth yearMonth, int stationId) throws IOException, InterruptedException {
        Path cachedFile = cacheDir.resolve(stationId + "/" + yearMonth.toString() + ".csv");
        cachedFile.getParent().toFile().mkdirs();

        if (Files.exists(cachedFile)) {
            log("Cache file already exists at: " + cachedFile.toAbsolutePath());
        } else {
            URI weatherDataUri = URI.create(getEnvironmentCanadaWeatherUrl(yearMonth, stationId));
            log("Downloading new weather data: " + cachedFile.toAbsolutePath());
            try (HttpClient httpClient = HttpClient.newHttpClient()) {
                HttpRequest request = HttpRequest.newBuilder(weatherDataUri)
                        .GET()
                        .build();

                HttpResponse<Path> response = httpClient.send(request, HttpResponse.BodyHandlers.ofFile(cachedFile));

                log("Response code %d for %s".formatted(response.statusCode(), response.body()));

                if (response.statusCode() != 200) {
                    throw new IOException("Failed to download weather data: HTTP " + response.statusCode());
                }
            } catch (Exception e) {
                IO.println("Failed with exception: " + e);
                throw e; // Re-throw to signal the error to the caller
            }
        }

        // Use Jackson CSV mapper to parse the CSV file
        CsvMapper csvMapper = new CsvMapper();
        csvMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // Skip the header row and configure the CSV format
        com.fasterxml.jackson.dataformat.csv.CsvSchema schema = csvMapper
                .schemaFor(WeatherDataPoint.class)
                .withHeader()
                .withColumnReordering(true)
                .withSkipFirstDataRow(false);

        ObjectReader reader = csvMapper.readerFor(WeatherDataPoint.class)
                .with(com.fasterxml.jackson.dataformat.csv.CsvParser.Feature.SKIP_EMPTY_LINES)
                .with(com.fasterxml.jackson.dataformat.csv.CsvParser.Feature.TRIM_SPACES)
                .with(com.fasterxml.jackson.dataformat.csv.CsvParser.Feature.IGNORE_TRAILING_UNMAPPABLE)
                .with(schema);

        try {
            // Read all records from the CSV file
            return reader.<WeatherDataPoint>readValues(cachedFile.toFile()).readAll();
        } catch (IOException e) {
            log("Error parsing CSV file: " + e.getMessage());
            // Fallback to the old method if parsing fails
            return Files.lines(cachedFile)
                    .skip(1) // Skip header row
                    .map(WeatherDataPoint::new)
                    .toList();
        }
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
