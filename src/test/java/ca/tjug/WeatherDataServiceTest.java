package ca.tjug;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.YearMonth;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the WeatherDataService class.
 */
class WeatherDataServiceTest {

    @TempDir
    Path tempDir;

    private WeatherDataService weatherDataService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        weatherDataService = new WeatherDataService(tempDir);
    }

    /**
     * Test that an exception is thrown when trying to fetch data for February.
     */
    @Test
    void fetchWeatherData_February_throwsException() {
        YearMonth february = YearMonth.of(2023, 2);
        int stationId = 31688;

        Exception exception = assertThrows(RuntimeException.class, () -> {
            weatherDataService.fetchWeatherData(february, stationId);
        });

        assertEquals("February has garbage weather", exception.getMessage());
    }

    /**
     * Test that data is correctly loaded from cache if it exists.
     */
    @Test
    void fetchWeatherData_cachedFileExists_loadsFromCache() throws IOException, InterruptedException {
        // Arrange
        YearMonth yearMonth = YearMonth.of(2023, 1);
        int stationId = 31688;
        
        // Create cache directory and file
        Path cacheDir = tempDir.resolve(String.valueOf(stationId));
        Files.createDirectories(cacheDir);
        
        Path cachedFile = cacheDir.resolve(yearMonth.toString() + ".csv");
        
        // Create a sample CSV file with header and one data row
        String csvContent = 
                "Date/Time,Year,Month,Day,Time,Temp (°C),Dew Point Temp (°C),Rel Hum (%),Wind Dir (10s deg),Wind Spd (km/h),Visibility (km),Stn Press (kPa),Weather\n" +
                "2023-01-01T00:00:00,2023,1,1,00:00:00,5.2,2.1,80,9,15.3,10.5,101.2,Cloudy";
        
        Files.writeString(cachedFile, csvContent);
        
        // Act
        List<WeatherDataPoint> result = weatherDataService.fetchWeatherData(yearMonth, stationId);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        
        WeatherDataPoint dataPoint = result.get(0);
        assertEquals("2023-01-01T00:00:00", dataPoint.dateTime());
        assertEquals(2023, dataPoint.year());
        assertEquals(1, dataPoint.month());
        assertEquals(1, dataPoint.day());
        assertEquals("00:00:00", dataPoint.time());
        assertEquals(5.2, dataPoint.temperature());
        assertEquals(2.1, dataPoint.dewPoint());
        assertEquals(80, dataPoint.relativeHumidity());
        assertEquals(9, dataPoint.windDirection());
        assertEquals(15.3, dataPoint.windSpeed());
        assertEquals(10.5, dataPoint.visibility());
        assertEquals(101.2, dataPoint.stationPressure());
        assertEquals("Cloudy", dataPoint.weather());
    }

    /**
     * Test that the getDateTime method correctly parses the date and time.
     */
    @Test
    void weatherDataPoint_getDateTime_parsesCorrectly() {
        // Arrange
        WeatherDataPoint dataPoint = new WeatherDataPoint(
                "2023-01-01T12:30:45", 2023, 1, 1, "12:30:45", 
                5.2, 2.1, 80, 9, 15.3, 10.5, 101.2, "Cloudy");
        
        // Act & Assert
        assertNotNull(dataPoint.getDateTime());
        assertEquals(2023, dataPoint.getDateTime().getYear());
        assertEquals(1, dataPoint.getDateTime().getMonthValue());
        assertEquals(1, dataPoint.getDateTime().getDayOfMonth());
        assertEquals(12, dataPoint.getDateTime().getHour());
        assertEquals(30, dataPoint.getDateTime().getMinute());
        assertEquals(45, dataPoint.getDateTime().getSecond());
    }
}