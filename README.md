# reparatio-sdk-java

Java SDK for the [Reparatio](https://reparatio.app) data conversion API.

**Requirements:** Java 11+ · No external runtime dependencies (uses JDK built-ins + `org.json`)

## Build

```bash
cd ~/reparatio-sdk-java
mvn package          # compile + run tests, produces target/reparatio-sdk-java-0.1.0.jar
mvn test             # run tests only
```

## Quick start

```java
import com.reparatio.ReparatioClient;
import java.nio.file.*;

try (var client = new ReparatioClient("rp_YOUR_KEY")) {
    // Inspect a file
    var info = client.inspect(Path.of("data.csv"));
    System.out.println(info.getInt("rows_total") + " rows");

    // Convert CSV → Parquet
    var result = client.convert(Path.of("data.csv"), "parquet");
    Files.write(Path.of(result.filename()), result.content());

    // SQL query
    var qr = client.query(Path.of("sales.csv"),
                          "SELECT region, SUM(revenue) FROM data GROUP BY region",
                          "json");

    // Merge two files (left join on id)
    var opts = new ReparatioClient.MergeOptions();
    opts.joinOn = "id";
    var merged = client.merge(Path.of("orders.csv"), Path.of("customers.csv"),
                              "left", "parquet", opts);

    // Append files vertically
    var appended = client.append(List.of(Path.of("jan.csv"), Path.of("feb.csv")), "csv");

    // Batch-convert a ZIP
    var batch = client.batchConvert(Path.of("archive.zip"), "parquet");
    if (batch.warning() != null) System.out.println("Errors: " + batch.warning());
}
```

The API key may also be supplied via the `REPARATIO_API_KEY` environment variable.

## API reference

### `new ReparatioClient(apiKey)`
### `new ReparatioClient(apiKey, baseUrl, timeout)`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `apiKey` | `$REPARATIO_API_KEY` | Your `rp_…` API key |
| `baseUrl` | `https://reparatio.app` | API root URL |
| `timeout` | `Duration.ofSeconds(120)` | HTTP timeout |

### `formats()` → `JSONObject`
Returns `input` and `output` format lists. No API key required.

### `me()` → `JSONObject`
Returns subscription and usage info.

### `inspect(path)` → `JSONObject`
Returns schema, encoding, row count, and preview rows. No API key required.

Options via `InspectOptions`: `noHeader`, `fixEncoding`, `previewRows`, `delimiter`, `sheet`.

### `convert(path, targetFormat)` → `ReparatioResult`
Options via `ConvertOptions`: `noHeader`, `fixEncoding`, `delimiter`, `sheet`,
`selectColumns`, `deduplicate`, `sampleN`, `sampleFrac`, `geometryColumn`,
`castColumnsJson`, `nullValues`, `encodingOverride`.

### `batchConvert(zipPath, targetFormat)` → `ReparatioResult`
Convert every file in a ZIP. Skipped files are listed in `result.warning()`.

### `merge(file1, file2, operation, targetFormat)` → `ReparatioResult`
`operation`: `"append"` `"left"` `"right"` `"outer"` `"inner"`.
Options via `MergeOptions`: `joinOn`, `noHeader`, `fixEncoding`, `geometryColumn`.

### `append(files, targetFormat)` → `ReparatioResult`
Stack ≥ 2 files vertically. Options via `AppendOptions`.

### `query(path, sql, targetFormat)` → `ReparatioResult`
Table name is always `data`. Options via `QueryOptions`.

### `ReparatioResult`
| Method | Type | Description |
|--------|------|-------------|
| `content()` | `byte[]` | Raw output file bytes |
| `filename()` | `String` | Suggested output filename |
| `warning()` | `String` / `null` | Server warning or skipped-file errors |

### Exceptions

| Class | HTTP | Extends |
|-------|------|---------|
| `ReparatioException.AuthenticationException` | 401, 403 | `ReparatioException` |
| `ReparatioException.InsufficientPlanException` | 402 | `ReparatioException` |
| `ReparatioException.FileTooLargeException` | 413 | `ReparatioException` |
| `ReparatioException.ParseException` | 422 | `ReparatioException` |
| `ReparatioException` | other 4xx/5xx | `RuntimeException` |

## License

MIT — © Ordo Artificum LLC
