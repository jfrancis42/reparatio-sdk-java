package com.reparatio;

import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Synchronous Reparatio API client.
 *
 * <p>Construct with an API key and call any of the API methods:
 * <pre>{@code
 * try (var client = new ReparatioClient("rp_YOUR_KEY")) {
 *     var info   = client.inspect(Path.of("data.csv"));
 *     var result = client.convert(Path.of("data.csv"), "parquet");
 *     Files.write(Path.of(result.filename()), result.content());
 * }
 * }</pre>
 *
 * <p>The API key may also be supplied via the {@code REPARATIO_API_KEY}
 * environment variable.
 */
public class ReparatioClient implements AutoCloseable {

    private static final String DEFAULT_BASE_URL = "https://reparatio.app";
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(120);
    private static final Pattern CD_FILENAME =
            Pattern.compile("filename=\"([^\"]+)\"");

    private final String        baseUrl;
    private final Map<String, String> defaultHeaders;
    private final Duration      timeout;
    private final HttpClient    http;

    // ── Constructors ──────────────────────────────────────────────────────

    /**
     * Create a client using the {@code REPARATIO_API_KEY} environment variable.
     */
    public ReparatioClient() {
        this(null, DEFAULT_BASE_URL, DEFAULT_TIMEOUT);
    }

    /**
     * Create a client with an explicit API key.
     *
     * @param apiKey  Your {@code rp_...} API key.
     */
    public ReparatioClient(String apiKey) {
        this(apiKey, DEFAULT_BASE_URL, DEFAULT_TIMEOUT);
    }

    /**
     * Full constructor.
     *
     * @param apiKey   API key; falls back to {@code REPARATIO_API_KEY} env var if null.
     * @param baseUrl  Override the API root (default: {@code https://reparatio.app}).
     * @param timeout  HTTP timeout (default: 120 s).
     */
    public ReparatioClient(String apiKey, String baseUrl, Duration timeout) {
        String key = apiKey != null ? apiKey :
                     System.getenv().getOrDefault("REPARATIO_API_KEY", "");
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.timeout = timeout;
        Map<String, String> hdrs = new LinkedHashMap<>();
        if (!key.isEmpty()) hdrs.put("X-API-Key", key);
        this.defaultHeaders = Collections.unmodifiableMap(hdrs);
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(timeout)
                .build();
    }

    @Override public void close() { /* HttpClient in Java 11 has no close(); added in Java 21 */ }

    // ── formats ───────────────────────────────────────────────────────────

    /**
     * List all supported input and output formats. No API key required.
     *
     * @return JSONObject with {@code input} and {@code output} arrays.
     */
    public JSONObject formats() {
        return getJson("/api/v1/formats");
    }

    // ── me ────────────────────────────────────────────────────────────────

    /**
     * Return subscription and usage details for the current API key.
     *
     * @return JSONObject with email, plan, api_access, request_count, etc.
     */
    public JSONObject me() {
        return getJson("/api/v1/me");
    }

    // ── inspect ───────────────────────────────────────────────────────────

    /**
     * Inspect a file: return schema, encoding, row count, and a data preview.
     * No API key required.
     *
     * @param filePath Path to the file to inspect.
     * @return JSONObject with filename, detected_encoding, rows_total, columns, preview.
     */
    public JSONObject inspect(Path filePath) {
        return inspect(filePath, new InspectOptions());
    }

    public JSONObject inspect(Path filePath, InspectOptions opts) {
        byte[] bytes = readFile(filePath);
        List<MultipartBody.Part> parts = new ArrayList<>();
        parts.add(new MultipartBody.Part("file", bytes, filePath.getFileName().toString(),
                                         "application/octet-stream"));
        parts.add(new MultipartBody.Part("no_header",    opts.noHeader    ? "true" : "false"));
        parts.add(new MultipartBody.Part("fix_encoding", opts.fixEncoding ? "true" : "false"));
        parts.add(new MultipartBody.Part("preview_rows", String.valueOf(opts.previewRows)));
        parts.add(new MultipartBody.Part("delimiter",    opts.delimiter));
        parts.add(new MultipartBody.Part("sheet",        opts.sheet));
        return postJson("/api/v1/inspect", parts);
    }

    // ── convert ───────────────────────────────────────────────────────────

    /**
     * Convert a file to a different format.
     *
     * @param filePath     Path to the source file.
     * @param targetFormat Output format, e.g. {@code "parquet"}, {@code "csv"}, {@code "xlsx"}.
     * @return ReparatioResult with content bytes and suggested filename.
     */
    public ReparatioResult convert(Path filePath, String targetFormat) {
        return convert(filePath, targetFormat, new ConvertOptions());
    }

    public ReparatioResult convert(Path filePath, String targetFormat, ConvertOptions opts) {
        byte[] bytes = readFile(filePath);
        String fname = filePath.getFileName().toString();
        List<MultipartBody.Part> parts = new ArrayList<>();
        parts.add(new MultipartBody.Part("file", bytes, fname, "application/octet-stream"));
        parts.add(new MultipartBody.Part("target_format",   targetFormat));
        parts.add(new MultipartBody.Part("no_header",       opts.noHeader    ? "true" : "false"));
        parts.add(new MultipartBody.Part("fix_encoding",    opts.fixEncoding ? "true" : "false"));
        parts.add(new MultipartBody.Part("delimiter",       opts.delimiter));
        parts.add(new MultipartBody.Part("sheet",           opts.sheet));
        parts.add(new MultipartBody.Part("select_columns",  toJsonArray(opts.selectColumns)));
        parts.add(new MultipartBody.Part("deduplicate",     opts.deduplicate ? "true" : "false"));
        parts.add(new MultipartBody.Part("sample_n",        String.valueOf(opts.sampleN)));
        parts.add(new MultipartBody.Part("sample_frac",     String.valueOf(opts.sampleFrac)));
        parts.add(new MultipartBody.Part("geometry_column", opts.geometryColumn));
        parts.add(new MultipartBody.Part("cast_columns",    opts.castColumnsJson));
        parts.add(new MultipartBody.Part("null_values",     toJsonArray(opts.nullValues)));
        if (opts.encodingOverride != null && !opts.encodingOverride.isEmpty())
            parts.add(new MultipartBody.Part("encoding_override", opts.encodingOverride));

        String baseName = stripExtension(fname);
        String fallback = baseName + "." + targetFormat;
        return postFile("/api/v1/convert", parts, fallback, "X-Reparatio-Warning");
    }

    // ── batch-convert ─────────────────────────────────────────────────────

    /**
     * Convert every file inside a ZIP archive to a common format.
     * Returns a ZIP of converted files. Files that fail are skipped;
     * their errors appear in {@link ReparatioResult#warning()}.
     *
     * @param zipPath      Path to the ZIP archive.
     * @param targetFormat Output format for every file, e.g. {@code "parquet"}.
     */
    public ReparatioResult batchConvert(Path zipPath, String targetFormat) {
        return batchConvert(zipPath, targetFormat, new BatchConvertOptions());
    }

    public ReparatioResult batchConvert(Path zipPath, String targetFormat,
                                        BatchConvertOptions opts) {
        byte[] bytes = readFile(zipPath);
        String fname = zipPath.getFileName().toString();
        List<MultipartBody.Part> parts = new ArrayList<>();
        parts.add(new MultipartBody.Part("zip_file", bytes, fname, "application/zip"));
        parts.add(new MultipartBody.Part("target_format",  targetFormat));
        parts.add(new MultipartBody.Part("no_header",      opts.noHeader    ? "true" : "false"));
        parts.add(new MultipartBody.Part("fix_encoding",   opts.fixEncoding ? "true" : "false"));
        parts.add(new MultipartBody.Part("delimiter",      opts.delimiter));
        parts.add(new MultipartBody.Part("select_columns", toJsonArray(opts.selectColumns)));
        parts.add(new MultipartBody.Part("deduplicate",    opts.deduplicate ? "true" : "false"));
        parts.add(new MultipartBody.Part("sample_n",       String.valueOf(opts.sampleN)));
        parts.add(new MultipartBody.Part("sample_frac",    String.valueOf(opts.sampleFrac)));
        parts.add(new MultipartBody.Part("cast_columns",   opts.castColumnsJson));
        return postFile("/api/v1/batch-convert", parts, "converted.zip", "X-Reparatio-Errors");
    }

    // ── merge ─────────────────────────────────────────────────────────────

    /**
     * Merge or join two files.
     *
     * @param file1     First file.
     * @param file2     Second file.
     * @param operation One of {@code "append"}, {@code "left"}, {@code "right"},
     *                  {@code "outer"}, {@code "inner"}.
     * @param targetFormat Output format.
     */
    public ReparatioResult merge(Path file1, Path file2,
                                  String operation, String targetFormat) {
        return merge(file1, file2, operation, targetFormat, new MergeOptions());
    }

    public ReparatioResult merge(Path file1, Path file2,
                                  String operation, String targetFormat,
                                  MergeOptions opts) {
        byte[] b1 = readFile(file1);
        byte[] b2 = readFile(file2);
        String f1 = file1.getFileName().toString();
        String f2 = file2.getFileName().toString();
        List<MultipartBody.Part> parts = new ArrayList<>();
        parts.add(new MultipartBody.Part("file1", b1, f1, "application/octet-stream"));
        parts.add(new MultipartBody.Part("file2", b2, f2, "application/octet-stream"));
        parts.add(new MultipartBody.Part("operation",       operation));
        parts.add(new MultipartBody.Part("target_format",   targetFormat));
        parts.add(new MultipartBody.Part("join_on",         opts.joinOn));
        parts.add(new MultipartBody.Part("no_header",       opts.noHeader    ? "true" : "false"));
        parts.add(new MultipartBody.Part("fix_encoding",    opts.fixEncoding ? "true" : "false"));
        parts.add(new MultipartBody.Part("geometry_column", opts.geometryColumn));
        String base1 = stripExtension(f1);
        String base2 = stripExtension(f2);
        String fallback = base1 + "_" + operation + "_" + base2 + "." + targetFormat;
        return postFile("/api/v1/merge", parts, fallback, "X-Reparatio-Warning");
    }

    // ── append ────────────────────────────────────────────────────────────

    /**
     * Stack rows from two or more files vertically (union / append).
     * Column mismatches are filled with null.
     *
     * @param files        List of file paths (minimum 2).
     * @param targetFormat Output format.
     */
    public ReparatioResult append(List<Path> files, String targetFormat) {
        return append(files, targetFormat, new AppendOptions());
    }

    public ReparatioResult append(List<Path> files, String targetFormat, AppendOptions opts) {
        if (files.size() < 2)
            throw new IllegalArgumentException("At least 2 files are required for append");
        List<MultipartBody.Part> parts = new ArrayList<>();
        for (Path p : files) {
            byte[] bytes = readFile(p);
            parts.add(new MultipartBody.Part("files", bytes,
                                              p.getFileName().toString(),
                                              "application/octet-stream"));
        }
        parts.add(new MultipartBody.Part("target_format", targetFormat));
        parts.add(new MultipartBody.Part("no_header",     opts.noHeader    ? "true" : "false"));
        parts.add(new MultipartBody.Part("fix_encoding",  opts.fixEncoding ? "true" : "false"));
        return postFile("/api/v1/append", parts,
                        "appended." + targetFormat, "X-Reparatio-Warning");
    }

    // ── query ─────────────────────────────────────────────────────────────

    /**
     * Run a SQL query against a file. The table is always named {@code data}.
     *
     * @param filePath     Source file.
     * @param sql          SQL query, e.g. {@code "SELECT region, SUM(revenue) FROM data GROUP BY region"}.
     * @param targetFormat Output format (default {@code "csv"}).
     */
    public ReparatioResult query(Path filePath, String sql, String targetFormat) {
        return query(filePath, sql, targetFormat, new QueryOptions());
    }

    public ReparatioResult query(Path filePath, String sql, String targetFormat,
                                  QueryOptions opts) {
        byte[] bytes = readFile(filePath);
        String fname = filePath.getFileName().toString();
        List<MultipartBody.Part> parts = new ArrayList<>();
        parts.add(new MultipartBody.Part("file", bytes, fname, "application/octet-stream"));
        parts.add(new MultipartBody.Part("sql",           sql));
        parts.add(new MultipartBody.Part("target_format", targetFormat));
        parts.add(new MultipartBody.Part("no_header",     opts.noHeader    ? "true" : "false"));
        parts.add(new MultipartBody.Part("fix_encoding",  opts.fixEncoding ? "true" : "false"));
        parts.add(new MultipartBody.Part("delimiter",     opts.delimiter));
        parts.add(new MultipartBody.Part("sheet",         opts.sheet));
        String fallback = stripExtension(fname) + "_query." + targetFormat;
        return postFile("/api/v1/query", parts, fallback, null);
    }

    // ── Options classes ───────────────────────────────────────────────────

    public static class InspectOptions {
        public boolean noHeader    = false;
        public boolean fixEncoding = true;
        public int     previewRows = 8;
        public String  delimiter   = "";
        public String  sheet       = "";
    }

    public static class ConvertOptions {
        public boolean      noHeader        = false;
        public boolean      fixEncoding     = true;
        public String       delimiter       = "";
        public String       sheet           = "";
        public List<String> selectColumns   = Collections.emptyList();
        public boolean      deduplicate     = false;
        public int          sampleN         = 0;
        public double       sampleFrac      = 0.0;
        public String       geometryColumn  = "geometry";
        public String       castColumnsJson = "{}";
        public List<String> nullValues      = Collections.emptyList();
        public String       encodingOverride = null;
    }

    public static class BatchConvertOptions {
        public boolean      noHeader      = false;
        public boolean      fixEncoding   = true;
        public String       delimiter     = "";
        public List<String> selectColumns = Collections.emptyList();
        public boolean      deduplicate   = false;
        public int          sampleN       = 0;
        public double       sampleFrac    = 0.0;
        public String       castColumnsJson = "{}";
    }

    public static class MergeOptions {
        public String  joinOn         = "";
        public boolean noHeader       = false;
        public boolean fixEncoding    = true;
        public String  geometryColumn = "geometry";
    }

    public static class AppendOptions {
        public boolean noHeader    = false;
        public boolean fixEncoding = true;
    }

    public static class QueryOptions {
        public boolean noHeader    = false;
        public boolean fixEncoding = true;
        public String  delimiter   = "";
        public String  sheet       = "";
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    private JSONObject getJson(String path) {
        try {
            HttpRequest.Builder req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + path))
                    .timeout(timeout)
                    .GET();
            defaultHeaders.forEach(req::header);

            HttpResponse<byte[]> resp = http.send(req.build(),
                                                   HttpResponse.BodyHandlers.ofByteArray());
            raiseForStatus(resp.statusCode(), resp.body());
            return new JSONObject(new String(resp.body(), StandardCharsets.UTF_8));
        } catch (ReparatioException e) {
            throw e;
        } catch (Exception e) {
            throw new ReparatioException(0, e.getMessage());
        }
    }

    private JSONObject postJson(String path, List<MultipartBody.Part> parts) {
        try {
            MultipartBody mp = new MultipartBody(parts);
            HttpRequest.Builder req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + path))
                    .timeout(timeout)
                    .header("Content-Type", mp.contentType())
                    .POST(HttpRequest.BodyPublishers.ofByteArray(mp.body()));
            defaultHeaders.forEach(req::header);

            HttpResponse<byte[]> resp = http.send(req.build(),
                                                   HttpResponse.BodyHandlers.ofByteArray());
            raiseForStatus(resp.statusCode(), resp.body());
            return new JSONObject(new String(resp.body(), StandardCharsets.UTF_8));
        } catch (ReparatioException e) {
            throw e;
        } catch (Exception e) {
            throw new ReparatioException(0, e.getMessage());
        }
    }

    private ReparatioResult postFile(String path, List<MultipartBody.Part> parts,
                                     String fallbackFilename, String warningHeader) {
        try {
            MultipartBody mp = new MultipartBody(parts);
            HttpRequest.Builder req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + path))
                    .timeout(timeout)
                    .header("Content-Type", mp.contentType())
                    .POST(HttpRequest.BodyPublishers.ofByteArray(mp.body()));
            defaultHeaders.forEach(req::header);

            HttpResponse<byte[]> resp = http.send(req.build(),
                                                   HttpResponse.BodyHandlers.ofByteArray());
            raiseForStatus(resp.statusCode(), resp.body());

            String filename = filenameFromHeaders(resp, fallbackFilename);
            String warning  = null;
            if (warningHeader != null) {
                Optional<String> wh = resp.headers().firstValue(warningHeader);
                if (wh.isPresent()) {
                    String raw = wh.get();
                    warning = warningHeader.equalsIgnoreCase("X-Reparatio-Errors")
                              ? URLDecoder.decode(raw, StandardCharsets.UTF_8)
                              : raw;
                }
            }
            return new ReparatioResult(resp.body(), filename, warning);
        } catch (ReparatioException e) {
            throw e;
        } catch (Exception e) {
            throw new ReparatioException(0, e.getMessage());
        }
    }

    private static void raiseForStatus(int status, byte[] body) {
        if (status < 400) return;
        String detail;
        try {
            JSONObject json = new JSONObject(new String(body, StandardCharsets.UTF_8));
            detail = json.optString("detail", new String(body, StandardCharsets.UTF_8));
        } catch (Exception e) {
            detail = new String(body, StandardCharsets.UTF_8);
        }
        switch (status) {
            case 401: case 403:
                throw new ReparatioException.AuthenticationException(status, detail);
            case 402:
                throw new ReparatioException.InsufficientPlanException(detail);
            case 413:
                throw new ReparatioException.FileTooLargeException(detail);
            case 422:
                throw new ReparatioException.ParseException(detail);
            default:
                throw new ReparatioException(status, detail);
        }
    }

    private static String filenameFromHeaders(HttpResponse<?> resp, String fallback) {
        Optional<String> cd = resp.headers().firstValue("Content-Disposition");
        if (cd.isPresent()) {
            Matcher m = CD_FILENAME.matcher(cd.get());
            if (m.find()) return m.group(1);
        }
        return fallback;
    }

    private static byte[] readFile(Path p) {
        try {
            return Files.readAllBytes(p);
        } catch (IOException e) {
            throw new ReparatioException(0, "Cannot read file: " + p + " — " + e.getMessage());
        }
    }

    private static String stripExtension(String filename) {
        int dot = filename.lastIndexOf('.');
        return dot > 0 ? filename.substring(0, dot) : filename;
    }

    private static String toJsonArray(List<String> list) {
        if (list == null || list.isEmpty()) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(list.get(i).replace("\"", "\\\"")).append('"');
        }
        sb.append(']');
        return sb.toString();
    }
}
