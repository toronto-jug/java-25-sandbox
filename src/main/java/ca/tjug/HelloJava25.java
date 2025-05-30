import ca.tjug.WeatherDataPoint;
import ca.tjug.WeatherDataService;

static final int START_YEAR = 2020;
static final int END_YEAR = 2024;
int stationId = 31688;

void main() throws IOException, InterruptedException {
    Path cacheDir = Path.of(System.getProperty("user.home"), ".tjug-weather");
    cacheDir.toFile().mkdirs();
    WeatherDataService weatherDataService = new WeatherDataService(cacheDir);

    List<YearMonth> yearMonths = Stream.iterate(START_YEAR, year -> year + 1)
            .takeWhile(year -> !year.equals(END_YEAR))
            .flatMap(year -> Stream.iterate(1, i -> i + 1)
                    .limit(12)
                    .map(month -> YearMonth.of(year, month)))
            .toList();

    Instant startTime = Instant.now();
    try (var scope = StructuredTaskScope.open()) {
        yearMonths.forEach(yearMonth -> {
            scope.fork(() -> weatherDataService.fetchWeatherData(yearMonth, stationId));
        });
        scope.join();
    } finally {
        Instant endTime = Instant.now();
        Duration duration = Duration.between(startTime, endTime);
        IO.println("Total time taken: " + duration);
    }
}

