import com.reparatio.ReparatioClient;
import com.reparatio.ReparatioException;
import com.reparatio.ReparatioResult;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Runnable examples for the Reparatio Java SDK.
 *
 * Compile and run:
 *   mvn -q package -DskipTests
 *   javac -cp target/reparatio-sdk-java-0.1.0.jar:~/.m2/repository/org/json/json/20240303/json-20240303.jar \
 *         examples/Examples.java -d examples/
 *   java -cp target/reparatio-sdk-java-0.1.0.jar:~/.m2/repository/org/json/json/20240303/json-20240303.jar:examples/ \
 *        Examples
 */
public class Examples {

    // ── Configuration ─────────────────────────────────────────────────────

    static final String API_KEY = System.getenv().getOrDefault("REPARATIO_API_KEY", "EXAMPLE-EXAMPLE-EXAMPLE");

    // ── Tracking ──────────────────────────────────────────────────────────

    static int passed = 0;
    static int failed = 0;
    static final List<String> failures = new ArrayList<>();

    // ── Helpers ───────────────────────────────────────────────────────────

    static void header(String name) {
        System.out.println("\n──────────────────────────────────────────");
        System.out.println("  " + name);
        System.out.println("──────────────────────────────────────────");
    }

    static void pass(String name) {
        System.out.println("  PASS");
        passed++;
    }

    static void fail(String name, String reason) {
        System.out.println("  FAIL: " + reason);
        failed++;
        failures.add(name + ": " + reason);
    }

    static void assertTrue(boolean condition, String msg) {
        if (!condition) throw new AssertionError(msg);
    }

    static void assertEquals(Object expected, Object actual, String msg) {
        if (!expected.equals(actual))
            throw new AssertionError(msg + " — expected " + expected + " but got " + actual);
    }

    // ── Inline CSV bytes for in-memory tests ──────────────────────────────

    static byte[] csvBytes(String csv) {
        return csv.getBytes(StandardCharsets.UTF_8);
    }

    // Build a ZIP in memory from multiple named CSV byte arrays
    static byte[] buildZip(String[] names, byte[][] contents) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(bos)) {
            for (int i = 0; i < names.length; i++) {
                zos.putNextEntry(new ZipEntry(names[i]));
                zos.write(contents[i]);
                zos.closeEntry();
            }
        }
        return bos.toByteArray();
    }

    // List filenames inside a ZIP byte array
    static List<String> zipEntryNames(byte[] zipBytes) throws IOException {
        List<String> names = new ArrayList<>();
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
            ZipEntry e;
            while ((e = zis.getNextEntry()) != null) {
                names.add(e.getName());
            }
        }
        return names;
    }

    // Write bytes to a temp file and return the path
    static Path writeTempFile(String prefix, String suffix, byte[] content) throws IOException {
        Path tmp = Files.createTempFile(prefix, suffix);
        Files.write(tmp, content);
        tmp.toFile().deleteOnExit();
        return tmp;
    }

    // ── Examples ──────────────────────────────────────────────────────────

    /**
     * Example 1: formats() — no API key needed.
     */
    static void exFormats() {
        String name = "formats()";
        header(name);
        try (var client = new ReparatioClient(null, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            JSONObject result = client.formats();
            JSONArray inputs  = result.getJSONArray("input");
            JSONArray outputs = result.getJSONArray("output");
            System.out.println("  Input  formats: " + inputs.length());
            System.out.println("  Output formats: " + outputs.length());

            // Verify expected formats are present
            List<String> inputList  = toStringList(inputs);
            List<String> outputList = toStringList(outputs);
            assertTrue(inputList.contains("csv"),     "csv must be an input format");
            assertTrue(inputList.contains("xlsx"),    "xlsx must be an input format");
            assertTrue(outputList.contains("parquet"),"parquet must be an output format");
            assertTrue(outputList.contains("jsonl"),  "jsonl must be an output format");

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 2: me() — account info.
     */
    static void exMe() {
        String name = "me()";
        header(name);
        try (var client = new ReparatioClient(API_KEY, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            JSONObject me = client.me();
            System.out.println("  email:         " + me.getString("email"));
            System.out.println("  plan:          " + me.getString("plan"));
            System.out.println("  api_access:    " + me.getBoolean("api_access"));
            System.out.println("  request_count: " + me.getInt("request_count"));

            assertTrue(me.getBoolean("api_access"), "api_access must be true");
            assertEquals("pro", me.getString("plan"), "plan");

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 3: inspect() — CSV from path.
     */
    static void exInspectCsvPath() {
        String name = "inspect() — CSV from path";
        header(name);
        try (var client = new ReparatioClient(API_KEY, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            byte[] csv = "country,county\nEngland,Kent\nEngland,Essex\nWales,Gwent\n"
                         .getBytes(StandardCharsets.UTF_8);
            Path tmp = writeTempFile("inspect_csv_", ".csv", csv);
            JSONObject info = client.inspect(tmp);
            System.out.println("  filename:          " + info.getString("filename"));
            System.out.println("  detected_encoding: " + info.getString("detected_encoding"));
            System.out.println("  rows:              " + info.getInt("rows"));
            System.out.println("  columns:           " + info.getJSONArray("columns").length());

            assertTrue(info.getInt("rows") > 0,     "rows must be positive");
            assertTrue(info.has("columns"),          "must have columns");
            assertTrue(info.has("preview"),          "must have preview");

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 4: inspect() — raw byte[] (in-memory CSV).
     */
    static void exInspectRawBytes() {
        String name = "inspect() — raw byte[] in-memory CSV";
        header(name);
        try (var client = new ReparatioClient(API_KEY, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            byte[] csv = csvBytes(
                "id,name,score\n" +
                "1,Alice,95\n"    +
                "2,Bob,82\n"      +
                "3,Carol,77\n"
            );
            // Write to a temp file and pass as path (SDK takes Path; bytes demo is via temp file)
            Path tmp = writeTempFile("inmemory_", ".csv", csv);
            JSONObject info = client.inspect(tmp);
            System.out.println("  rows:    " + info.getInt("rows"));
            System.out.println("  columns: " + info.getJSONArray("columns").length());

            assertEquals(3,    info.getInt("rows"),                    "row count");
            assertEquals(3,    info.getJSONArray("columns").length(),  "column count");

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 5: inspect() — TSV with multiple columns.
     */
    static void exInspectTsvColumns() {
        String name = "inspect() — TSV with multiple columns";
        header(name);
        try (var client = new ReparatioClient(API_KEY, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            byte[] tsv = "id\tname\tdept\tsalary\n1\tAlice\tEng\t90000\n2\tBob\tMkt\t75000\n3\tCarol\tEng\t85000\n"
                         .getBytes(StandardCharsets.UTF_8);
            Path tmp = writeTempFile("inspect_tsv_", ".tsv", tsv);
            JSONObject info = client.inspect(tmp);
            System.out.println("  filename: " + info.getString("filename"));
            System.out.println("  rows:     " + info.getInt("rows"));
            System.out.println("  columns:  " + info.getJSONArray("columns").length());

            assertTrue(info.getInt("rows") > 0, "must have rows");
            assertTrue(info.has("columns"),      "must have columns");
            assertTrue(info.has("preview"),      "must have preview");

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 6: convert() — CSV → Parquet, verify PAR1 magic bytes.
     */
    static void exConvertCsvToParquet() {
        String name = "convert() — CSV to Parquet";
        header(name);
        try (var client = new ReparatioClient(API_KEY, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            byte[] csv = "country,county\nEngland,Kent\nEngland,Essex\nWales,Gwent\n"
                         .getBytes(StandardCharsets.UTF_8);
            Path tmp = writeTempFile("convert_parquet_", ".csv", csv);
            ReparatioResult result = client.convert(tmp, "parquet");
            byte[] content = result.content();
            System.out.println("  filename:  " + result.filename());
            System.out.println("  file size: " + content.length + " bytes");
            System.out.println("  magic:     " + new String(Arrays.copyOfRange(content, 0, 4),
                                                              StandardCharsets.US_ASCII));

            // Parquet files start AND end with PAR1
            byte[] magic = Arrays.copyOfRange(content, 0, 4);
            assertTrue(Arrays.equals(magic, "PAR1".getBytes(StandardCharsets.US_ASCII)),
                       "File must start with PAR1 magic bytes");
            assertTrue(result.filename().endsWith(".parquet"), "filename must end with .parquet");

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 7: convert() — CSV → JSON Lines.
     */
    static void exConvertCsvToJsonl() {
        String name = "convert() — CSV to JSON Lines";
        header(name);
        try (var client = new ReparatioClient(API_KEY, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            byte[] csv = "id,name,score\n1,Alice,95\n2,Bob,82\n3,Carol,77\n"
                         .getBytes(StandardCharsets.UTF_8);
            Path tmp = writeTempFile("convert_jsonl_", ".csv", csv);
            ReparatioResult result = client.convert(tmp, "jsonl");
            String text = new String(result.content(), StandardCharsets.UTF_8);
            String[] lines = text.strip().split("\n");
            System.out.println("  filename:   " + result.filename());
            System.out.println("  lines:      " + lines.length);
            System.out.println("  first line: " + lines[0].substring(0, Math.min(80, lines[0].length())));

            assertTrue(lines.length > 0, "must have at least one line");
            // Each line must be valid JSON
            new JSONObject(lines[0]);   // throws if invalid
            assertTrue(result.filename().endsWith(".jsonl"), "filename must end with .jsonl");

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 8: convert() — select + rename columns + gzip output (csv.gz).
     */
    static void exConvertSelectGzip() {
        String name = "convert() — select columns + csv.gz output";
        header(name);
        try (var client = new ReparatioClient(API_KEY, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            byte[] csv = "country,county\nEngland,Kent\nEngland,Essex\nWales,Gwent\n"
                         .getBytes(StandardCharsets.UTF_8);
            Path tmp = writeTempFile("convert_gz_", ".csv", csv);
            var opts = new ReparatioClient.ConvertOptions();
            opts.selectColumns = List.of("country", "county");

            ReparatioResult result = client.convert(tmp, "csv.gz", opts);
            byte[] content = result.content();
            System.out.println("  filename:  " + result.filename());
            System.out.println("  file size: " + content.length + " bytes");

            // gzip magic bytes: 0x1f 0x8b
            assertTrue(content.length > 2, "output must not be empty");
            assertTrue((content[0] & 0xFF) == 0x1f && (content[1] & 0xFF) == 0x8b,
                       "File must start with gzip magic bytes 0x1f 0x8b");
            assertTrue(result.filename().endsWith(".gz"), "filename must end with .gz");

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 9: convert() — deduplicate + sample_n rows.
     */
    static void exConvertDeduplicateSample() {
        String name = "convert() — deduplicate + sample_n";
        header(name);
        try (var client = new ReparatioClient(API_KEY, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            // CSV with 4 unique countries across many rows
            byte[] csv = csvBytes(
                "country\n" +
                "England\nEngland\nEngland\nEngland\nEngland\n" +
                "Wales\nWales\nWales\n" +
                "Scotland\nScotland\nScotland\nScotland\n" +
                "Ireland\nIreland\n"
            );
            Path tmp = writeTempFile("convert_dedup_", ".csv", csv);
            var opts = new ReparatioClient.ConvertOptions();
            opts.selectColumns = List.of("country");
            opts.deduplicate   = true;
            // After dedup there are 4 unique countries; sample_n caps at what's available
            opts.sampleN       = 3;

            ReparatioResult result = client.convert(tmp, "csv", opts);
            String text  = new String(result.content(), StandardCharsets.UTF_8).strip();
            String[] lines = text.split("\n");
            // 1 header + up to 3 data rows
            System.out.println("  filename: " + result.filename());
            System.out.println("  lines (header + data): " + lines.length);

            assertTrue(lines.length <= 4, "dedup + sample_n=3 must yield at most 4 lines (header + 3 data)");
            assertTrue(lines.length >= 2, "must have at least header + 1 data row");

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 10: convert() — castColumns type overrides.
     */
    static void exConvertCastColumns() {
        String name = "convert() — castColumns type overrides";
        header(name);
        try (var client = new ReparatioClient(API_KEY, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            byte[] csv = "country,county\nEngland,Kent\nEngland,Essex\nWales,Gwent\n"
                         .getBytes(StandardCharsets.UTF_8);
            Path tmp = writeTempFile("convert_cast_", ".csv", csv);
            var opts = new ReparatioClient.ConvertOptions();
            // Cast both columns to String explicitly
            opts.castColumnsJson = "{\"country\":\"String\",\"county\":\"String\"}";

            ReparatioResult result = client.convert(tmp, "json", opts);
            String json = new String(result.content(), StandardCharsets.UTF_8);
            JSONArray rows = new JSONArray(json);
            System.out.println("  filename: " + result.filename());
            System.out.println("  rows:     " + rows.length());
            System.out.println("  sample:   " + rows.getJSONObject(0));

            assertTrue(rows.length() > 0, "must have rows");
            JSONObject first = rows.getJSONObject(0);
            assertTrue(first.has("country"), "must have country column");
            assertTrue(first.has("county"),  "must have county column");
            // After cast to String, values are strings
            assertTrue(first.get("country") instanceof String, "country must be a String");

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 11: query() — SQL aggregation, parse result JSON.
     */
    static void exQuery() {
        String name = "query() — SQL aggregation";
        header(name);
        try (var client = new ReparatioClient(API_KEY, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            byte[] csv = csvBytes(
                "country,county\n" +
                "England,Kent\nEngland,Essex\nEngland,Surrey\nEngland,Sussex\n" +
                "Wales,Gwent\nWales,Powys\n" +
                "Scotland,Fife\nScotland,Angus\nScotland,Perth\n" +
                "Ireland,Cork\n"
            );
            Path tmp = writeTempFile("query_", ".csv", csv);
            String sql = "SELECT country, COUNT(*) AS county_count " +
                         "FROM data " +
                         "GROUP BY country " +
                         "ORDER BY county_count DESC";

            ReparatioResult result = client.query(tmp, sql, "json");
            String json = new String(result.content(), StandardCharsets.UTF_8);
            JSONArray rows = new JSONArray(json);
            System.out.println("  filename:      " + result.filename());
            System.out.println("  groups:        " + rows.length());
            for (int i = 0; i < rows.length(); i++) {
                JSONObject row = rows.getJSONObject(i);
                System.out.printf("  %-20s %d%n",
                    row.getString("country"), row.getInt("county_count"));
            }

            assertEquals(4, rows.length(), "must have 4 country groups");
            assertEquals("England", rows.getJSONObject(0).getString("country"),
                         "England must be first (most counties)");
            assertTrue(rows.getJSONObject(0).getInt("county_count") > 3,
                       "England must have more than 3 counties");

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 12: append() — stack three in-memory CSVs.
     */
    static void exAppend() {
        String name = "append() — stack three in-memory CSVs";
        header(name);
        try (var client = new ReparatioClient(API_KEY, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            byte[] a = csvBytes("id,name\n1,Alice\n2,Bob\n");
            byte[] b = csvBytes("id,name\n3,Carol\n4,Dave\n");
            byte[] c = csvBytes("id,name\n5,Eve\n");

            Path pa = writeTempFile("append_a_", ".csv", a);
            Path pb = writeTempFile("append_b_", ".csv", b);
            Path pc = writeTempFile("append_c_", ".csv", c);

            ReparatioResult result = client.append(List.of(pa, pb, pc), "csv");
            String text  = new String(result.content(), StandardCharsets.UTF_8).strip();
            String[] lines = text.split("\n");
            System.out.println("  filename:     " + result.filename());
            System.out.println("  total lines:  " + lines.length + "  (header + data rows)");
            for (String line : lines) System.out.println("    " + line);

            // 1 header + 5 data rows = 6 lines
            assertEquals(6, lines.length, "must have 6 lines (1 header + 5 data rows)");
            assertEquals("id,name", lines[0], "header row");

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 13: merge() — inner join two CSVs on a key column.
     */
    static void exMerge() {
        String name = "merge() — inner join two CSVs";
        header(name);
        try (var client = new ReparatioClient(API_KEY, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            // employees: id, name, dept_id
            byte[] employees = csvBytes(
                "id,name,dept_id\n" +
                "1,Alice,10\n"      +
                "2,Bob,20\n"        +
                "3,Carol,10\n"      +
                "4,Dave,30\n"       // dept 30 has no match → excluded in inner join
            );
            // departments: dept_id, dept_name
            byte[] departments = csvBytes(
                "dept_id,dept_name\n" +
                "10,Engineering\n"    +
                "20,Marketing\n"
                // dept 30 absent → Dave excluded from inner join
            );

            Path pEmp  = writeTempFile("emp_",  ".csv", employees);
            Path pDept = writeTempFile("dept_", ".csv", departments);

            var opts = new ReparatioClient.MergeOptions();
            opts.joinOn = "dept_id";

            ReparatioResult result = client.merge(pEmp, pDept, "inner", "json", opts);
            String json = new String(result.content(), StandardCharsets.UTF_8);
            JSONArray rows = new JSONArray(json);
            System.out.println("  filename:     " + result.filename());
            System.out.println("  joined rows:  " + rows.length());
            for (int i = 0; i < rows.length(); i++) {
                System.out.println("  " + rows.getJSONObject(i));
            }

            // Alice (dept 10), Bob (dept 20), Carol (dept 10) → 3 rows; Dave is excluded
            assertEquals(3, rows.length(), "inner join must produce 3 rows");
            // All rows must have dept_name
            for (int i = 0; i < rows.length(); i++) {
                assertTrue(rows.getJSONObject(i).has("dept_name"),
                           "every row must have dept_name");
            }

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 14: batchConvert() — ZIP of CSVs → ZIP of Parquets.
     */
    static void exBatchConvert() {
        String name = "batchConvert() — ZIP of CSVs to ZIP of Parquets";
        header(name);
        try (var client = new ReparatioClient(API_KEY, "https://reparatio.app", java.time.Duration.ofSeconds(60))) {
            byte[] csv1 = csvBytes("country,capital\nFrance,Paris\nGermany,Berlin\n");
            byte[] csv2 = csvBytes("lang,family\nJava,JVM\nScala,JVM\nPython,CPython\n");
            byte[] csv3 = csvBytes("country,county\nEngland,Kent\nEngland,Essex\nWales,Gwent\n");

            byte[] zipBytes = buildZip(
                new String[]{"countries.csv", "langs.csv", "counties.csv"},
                new byte[][]{csv1, csv2, csv3}
            );
            Path zipPath = writeTempFile("batch_", ".zip", zipBytes);

            ReparatioResult result = client.batchConvert(zipPath, "parquet");
            List<String> entries = zipEntryNames(result.content());
            System.out.println("  filename:      " + result.filename());
            System.out.println("  output entries: " + entries);

            assertTrue(entries.size() == 3, "must have 3 output files");
            assertTrue(entries.contains("countries.parquet"), "must contain countries.parquet");
            assertTrue(entries.contains("langs.parquet"),     "must contain langs.parquet");
            assertTrue(entries.contains("counties.parquet"),  "must contain counties.parquet");

            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    /**
     * Example 15: error handling — bad key throws AuthenticationException.
     */
    static void exErrorHandlingBadKey() {
        String name = "error handling — bad API key";
        header(name);
        try (var client = new ReparatioClient("bad-key-xyz", "https://reparatio.app",
                                               java.time.Duration.ofSeconds(60))) {
            try {
                client.me();
                fail(name, "Expected AuthenticationException but no exception was thrown");
                return;
            } catch (ReparatioException.AuthenticationException ex) {
                System.out.println("  Caught AuthenticationException as expected");
                System.out.println("  status code: " + ex.getStatusCode());
                System.out.println("  message:     " + ex.getMessage());

                assertTrue(ex.getStatusCode() == 401 || ex.getStatusCode() == 403,
                           "status code must be 401 or 403, got " + ex.getStatusCode());
            }
            pass(name);
        } catch (AssertionError | Exception e) {
            fail(name, e.getMessage());
        }
    }

    // ── Utility ───────────────────────────────────────────────────────────

    static List<String> toStringList(JSONArray arr) {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < arr.length(); i++) list.add(arr.getString(i));
        return list;
    }

    // ── main ──────────────────────────────────────────────────────────────

    public static void main(String[] args) {
        System.out.println("Reparatio Java SDK — Examples");

        exFormats();
        exMe();
        exInspectCsvPath();
        exInspectRawBytes();
        exInspectTsvColumns();
        exConvertCsvToParquet();
        exConvertCsvToJsonl();
        exConvertSelectGzip();
        exConvertDeduplicateSample();
        exConvertCastColumns();
        exQuery();
        exAppend();
        exMerge();
        exBatchConvert();
        exErrorHandlingBadKey();

        System.out.println("\n══════════════════════════════════════════");
        System.out.printf("Results: %d/%d passed%n", passed, passed + failed);
        if (!failures.isEmpty()) {
            System.out.println("Failures:");
            failures.forEach(f -> System.out.println("  - " + f));
        }
        System.out.println("══════════════════════════════════════════");

        if (failed > 0) System.exit(1);
    }
}
