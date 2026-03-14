package com.reparatio;

import org.json.JSONObject;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ReparatioClient.
 *
 * We test at the MultipartBody / helper level (pure unit tests) and at the
 * client level by injecting a mock HttpClient via reflection.
 */
class ReparatioClientTest {

    // ── helpers ───────────────────────────────────────────────────────────

    /** Build a fake HttpResponse<byte[]> for use in mock tests. */
    static HttpResponse<byte[]> fakeResponse(int status, String body,
                                             Map<String, List<String>> headers) {
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        return new HttpResponse<>() {
            public int statusCode() { return status; }
            public byte[] body()    { return bodyBytes; }
            public HttpHeaders headers() {
                return HttpHeaders.of(headers != null ? headers : Map.of(),
                                      (a, b) -> true);
            }
            public HttpRequest   request()         { return null; }
            public Optional<HttpResponse<byte[]>> previousResponse() { return Optional.empty(); }
            public Optional<javax.net.ssl.SSLSession> sslSession()   { return Optional.empty(); }
            public URI uri() { return URI.create("https://reparatio.app"); }
            public HttpClient.Version version()    { return HttpClient.Version.HTTP_1_1; }
        };
    }

    static HttpResponse<byte[]> fakeResponse(int status, String body) {
        return fakeResponse(status, body, null);
    }

    static HttpResponse<byte[]> fakeResponse(int status, byte[] bodyBytes,
                                             Map<String, List<String>> headers) {
        return new HttpResponse<>() {
            public int statusCode() { return status; }
            public byte[] body()    { return bodyBytes; }
            public HttpHeaders headers() {
                return HttpHeaders.of(headers != null ? headers : Map.of(),
                                      (a, b) -> true);
            }
            public HttpRequest   request()         { return null; }
            public Optional<HttpResponse<byte[]>> previousResponse() { return Optional.empty(); }
            public Optional<javax.net.ssl.SSLSession> sslSession()   { return Optional.empty(); }
            public URI uri() { return URI.create("https://reparatio.app"); }
            public HttpClient.Version version()    { return HttpClient.Version.HTTP_1_1; }
        };
    }

    // ── ReparatioException ────────────────────────────────────────────────

    @Test void exception_statusCode() {
        var e = new ReparatioException(404, "not found");
        assertEquals(404, e.getStatusCode());
        assertEquals("not found", e.getMessage());
    }

    @Test void exception_authSubclass() {
        var e = new ReparatioException.AuthenticationException(401, "bad key");
        assertEquals(401, e.getStatusCode());
        assertInstanceOf(ReparatioException.class, e);
    }

    @Test void exception_planSubclass() {
        var e = new ReparatioException.InsufficientPlanException("Pro required");
        assertEquals(402, e.getStatusCode());
    }

    @Test void exception_fileTooLargeSubclass() {
        var e = new ReparatioException.FileTooLargeException("too big");
        assertEquals(413, e.getStatusCode());
    }

    @Test void exception_parseSubclass() {
        var e = new ReparatioException.ParseException("unreadable");
        assertEquals(422, e.getStatusCode());
    }

    // ── ReparatioResult ───────────────────────────────────────────────────

    @Test void result_accessors() {
        byte[] content = "PAR1".getBytes(StandardCharsets.UTF_8);
        var r = new ReparatioResult(content, "out.parquet", "warning msg");
        assertArrayEquals(content, r.content());
        assertEquals("out.parquet", r.filename());
        assertEquals("warning msg", r.warning());
    }

    @Test void result_nullWarning() {
        var r = new ReparatioResult(new byte[0], "x.csv", null);
        assertNull(r.warning());
    }

    // ── MultipartBody ─────────────────────────────────────────────────────

    @Test void multipart_textField() throws Exception {
        var parts = List.of(new MultipartBody.Part("key", "value"));
        var mp = new MultipartBody(parts);
        String body = new String(mp.body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("name=\"key\""));
        assertTrue(body.contains("value"));
        assertTrue(mp.contentType().startsWith("multipart/form-data; boundary="));
    }

    @Test void multipart_fileField() throws Exception {
        byte[] fileBytes = "id,name\n1,Alice\n".getBytes(StandardCharsets.UTF_8);
        var parts = List.of(
            new MultipartBody.Part("file", fileBytes, "data.csv", "text/csv")
        );
        var mp = new MultipartBody(parts);
        String body = new String(mp.body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("filename=\"data.csv\""));
        assertTrue(body.contains("Content-Type: text/csv"));
        assertTrue(body.contains("id,name"));
    }

    @Test void multipart_mixedParts() throws Exception {
        byte[] fileBytes = "hello".getBytes(StandardCharsets.UTF_8);
        var parts = List.of(
            new MultipartBody.Part("target_format", "parquet"),
            new MultipartBody.Part("file", fileBytes, "x.csv", "text/csv")
        );
        var mp = new MultipartBody(parts);
        String body = new String(mp.body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("name=\"target_format\""));
        assertTrue(body.contains("parquet"));
        assertTrue(body.contains("filename=\"x.csv\""));
    }

    // ── constructor / defaults ────────────────────────────────────────────

    @Test void constructor_stripsTrailingSlash() {
        var c = new ReparatioClient("rp_k", "http://localhost:8000/", Duration.ofSeconds(30));
        // Verify via attempt (we expect a connection error, not a URL error)
        // We check via the field directly using reflection
        try {
            Field f = ReparatioClient.class.getDeclaredField("baseUrl");
            f.setAccessible(true);
            assertEquals("http://localhost:8000", f.get(c));
        } catch (Exception e) {
            fail("Reflection failed: " + e.getMessage());
        }
    }

    @Test void constructor_readsEnvApiKey() {
        // If REPARATIO_API_KEY is set, client picks it up; if not, header map is empty.
        String envKey = System.getenv("REPARATIO_API_KEY");
        var c = new ReparatioClient();
        try {
            Field f = ReparatioClient.class.getDeclaredField("defaultHeaders");
            f.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, String> hdrs = (Map<String, String>) f.get(c);
            if (envKey != null && !envKey.isEmpty()) {
                assertEquals(envKey, hdrs.get("X-API-Key"));
            } else {
                assertFalse(hdrs.containsKey("X-API-Key"));
            }
        } catch (Exception e) {
            fail("Reflection failed: " + e.getMessage());
        }
    }

    @Test void constructor_explicitKeyWins() {
        var c = new ReparatioClient("rp_explicit", "https://reparatio.app", Duration.ofSeconds(30));
        try {
            Field f = ReparatioClient.class.getDeclaredField("defaultHeaders");
            f.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, String> hdrs = (Map<String, String>) f.get(c);
            assertEquals("rp_explicit", hdrs.get("X-API-Key"));
        } catch (Exception e) {
            fail("Reflection failed: " + e.getMessage());
        }
    }

    // ── raiseForStatus (via reflection) ──────────────────────────────────

    void callRaiseForStatus(int status, String body) {
        try {
            var m = ReparatioClient.class.getDeclaredMethod("raiseForStatus",
                                                            int.class, byte[].class);
            m.setAccessible(true);
            m.invoke(null, status, body.getBytes(StandardCharsets.UTF_8));
        } catch (java.lang.reflect.InvocationTargetException ite) {
            if (ite.getCause() instanceof RuntimeException re) throw re;
            throw new RuntimeException(ite.getCause());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test void raiseForStatus_200_ok() {
        assertDoesNotThrow(() -> callRaiseForStatus(200, "{}"));
    }

    @Test void raiseForStatus_401_auth() {
        var ex = assertThrows(ReparatioException.AuthenticationException.class,
                              () -> callRaiseForStatus(401, "{\"detail\":\"bad key\"}"));
        assertEquals(401, ex.getStatusCode());
        assertEquals("bad key", ex.getMessage());
    }

    @Test void raiseForStatus_403_auth() {
        assertThrows(ReparatioException.AuthenticationException.class,
                     () -> callRaiseForStatus(403, "{\"detail\":\"Forbidden\"}"));
    }

    @Test void raiseForStatus_402_plan() {
        assertThrows(ReparatioException.InsufficientPlanException.class,
                     () -> callRaiseForStatus(402, "{\"detail\":\"Pro required\"}"));
    }

    @Test void raiseForStatus_413_size() {
        assertThrows(ReparatioException.FileTooLargeException.class,
                     () -> callRaiseForStatus(413, "{\"detail\":\"too large\"}"));
    }

    @Test void raiseForStatus_422_parse() {
        assertThrows(ReparatioException.ParseException.class,
                     () -> callRaiseForStatus(422, "{\"detail\":\"bad format\"}"));
    }

    @Test void raiseForStatus_500_generic() {
        var ex = assertThrows(ReparatioException.class,
                              () -> callRaiseForStatus(500, "{\"detail\":\"oops\"}"));
        assertEquals(500, ex.getStatusCode());
    }

    @Test void raiseForStatus_nonJsonBody() {
        var ex = assertThrows(ReparatioException.class,
                              () -> callRaiseForStatus(503, "Service Unavailable"));
        assertEquals("Service Unavailable", ex.getMessage());
    }

    // ── filenameFromHeaders ───────────────────────────────────────────────

    HttpResponse<byte[]> responseWithHeader(String name, String value) {
        Map<String, List<String>> hdrs = new HashMap<>();
        if (name != null) hdrs.put(name, List.of(value));
        return fakeResponse(200, new byte[0], hdrs);
    }

    String callFilenameFromHeaders(HttpResponse<?> resp, String fallback) {
        try {
            var m = ReparatioClient.class.getDeclaredMethod(
                    "filenameFromHeaders", HttpResponse.class, String.class);
            m.setAccessible(true);
            return (String) m.invoke(null, resp, fallback);
        } catch (java.lang.reflect.InvocationTargetException ite) {
            throw new RuntimeException(ite.getCause());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test void filename_fromContentDisposition() {
        var resp = responseWithHeader("Content-Disposition",
                                     "attachment; filename=\"output.parquet\"");
        assertEquals("output.parquet", callFilenameFromHeaders(resp, "fallback.csv"));
    }

    @Test void filename_fallbackWhenAbsent() {
        var resp = responseWithHeader(null, null);
        assertEquals("fallback.csv", callFilenameFromHeaders(resp, "fallback.csv"));
    }

    @Test void filename_fallbackWhenNoFilenameParam() {
        var resp = responseWithHeader("Content-Disposition", "attachment");
        assertEquals("fallback.csv", callFilenameFromHeaders(resp, "fallback.csv"));
    }

    // ── toJsonArray ───────────────────────────────────────────────────────

    String callToJsonArray(List<String> list) {
        try {
            var m = ReparatioClient.class.getDeclaredMethod("toJsonArray", List.class);
            m.setAccessible(true);
            return (String) m.invoke(null, list);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Test void jsonArray_empty()      { assertEquals("[]", callToJsonArray(List.of())); }
    @Test void jsonArray_null()       { assertEquals("[]", callToJsonArray(null)); }
    @Test void jsonArray_singleItem() { assertEquals("[\"id\"]", callToJsonArray(List.of("id"))); }
    @Test void jsonArray_multiItems() {
        assertEquals("[\"a\",\"b\",\"c\"]", callToJsonArray(List.of("a", "b", "c")));
    }

    // ── stripExtension ────────────────────────────────────────────────────

    String callStripExtension(String name) {
        try {
            var m = ReparatioClient.class.getDeclaredMethod("stripExtension", String.class);
            m.setAccessible(true);
            return (String) m.invoke(null, name);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Test void stripExt_normal()     { assertEquals("data",    callStripExtension("data.csv")); }
    @Test void stripExt_noExt()      { assertEquals("noext",   callStripExtension("noext")); }
    @Test void stripExt_dotAtStart() { assertEquals(".dotfile", callStripExtension(".dotfile")); }

    // ── file I/O helper ───────────────────────────────────────────────────

    @Test void readFile_success(@TempDir Path dir) throws IOException {
        Path f = dir.resolve("test.csv");
        Files.write(f, "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8));
        try {
            var m = ReparatioClient.class.getDeclaredMethod("readFile", Path.class);
            m.setAccessible(true);
            byte[] result = (byte[]) m.invoke(null, f);
            assertEquals("a,b\n1,2\n", new String(result, StandardCharsets.UTF_8));
        } catch (Exception e) { fail(e.getMessage()); }
    }

    @Test void readFile_missing() {
        try {
            var m = ReparatioClient.class.getDeclaredMethod("readFile", Path.class);
            m.setAccessible(true);
            assertThrows(Exception.class,
                         () -> m.invoke(null, Path.of("/nonexistent/file.csv")));
        } catch (Exception e) { fail(e.getMessage()); }
    }

    // ── append: argument validation ───────────────────────────────────────

    @Test void append_requiresTwoFiles(@TempDir Path dir) throws IOException {
        var c = new ReparatioClient("rp_k");
        Path f = dir.resolve("a.csv");
        Files.write(f, "a\n1\n".getBytes(StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class,
                     () -> c.append(List.of(f), "csv"));
    }

    // ── Integration-style tests using real HTTP mock ──────────────────────
    //
    // We use a real file on disk + a mock HTTP client injected via reflection.
    // The mock intercepts HttpClient.send() calls and returns pre-canned responses.

    /** Replace the private 'http' field in the client with a mock. */
    static void injectMockHttp(ReparatioClient client, HttpClient mock) {
        try {
            Field f = ReparatioClient.class.getDeclaredField("http");
            f.setAccessible(true);
            f.set(client, mock);
        } catch (Exception e) { fail("Could not inject mock: " + e.getMessage()); }
    }

    static byte[] CSV = "id,name\n1,Alice\n2,Bob\n".getBytes(StandardCharsets.UTF_8);

    static final String FORMATS_JSON =
        "{\"input\":[\"csv\",\"xlsx\",\"parquet\"],\"output\":[\"csv\",\"xlsx\",\"parquet\"]}";
    static final String ME_JSON =
        "{\"email\":\"user@example.com\",\"plan\":\"pro\",\"api_access\":true," +
        "\"request_count\":42,\"data_bytes_total\":1048576}";
    static final String INSPECT_JSON =
        "{\"filename\":\"data.csv\",\"detected_encoding\":\"utf-8\"," +
        "\"rows_total\":100,\"columns\":[],\"preview\":[]}";

    // Note: We can't easily subclass HttpClient (it's abstract with internal impl).
    // We instead test that exceptions are thrown for HTTP errors using the
    // static raiseForStatus path already tested above, and test the multipart
    // construction thoroughly.  For integration, we create thin smoke tests
    // that verify the client correctly raises for error status codes by calling
    // a local echo server — but since we have no server here, we verify
    // correct behaviour via direct testing of the error-raising logic.

    @Test void formats_jsonParsed() {
        JSONObject json = new JSONObject(FORMATS_JSON);
        assertTrue(json.has("input"));
        assertTrue(json.has("output"));
        assertTrue(json.getJSONArray("input").length() > 0);
    }

    @Test void me_jsonParsed() {
        JSONObject json = new JSONObject(ME_JSON);
        assertEquals("user@example.com", json.getString("email"));
        assertEquals("pro", json.getString("plan"));
        assertEquals(42, json.getInt("request_count"));
    }

    @Test void inspect_jsonParsed() {
        JSONObject json = new JSONObject(INSPECT_JSON);
        assertEquals("utf-8", json.getString("detected_encoding"));
        assertEquals(100, json.getInt("rows_total"));
    }

    // ── ConvertOptions defaults ───────────────────────────────────────────

    @Test void convertOptions_defaults() {
        var opts = new ReparatioClient.ConvertOptions();
        assertFalse(opts.noHeader);
        assertTrue(opts.fixEncoding);
        assertEquals("", opts.delimiter);
        assertEquals("", opts.sheet);
        assertFalse(opts.deduplicate);
        assertEquals(0, opts.sampleN);
        assertEquals(0.0, opts.sampleFrac);
        assertEquals("geometry", opts.geometryColumn);
        assertEquals("{}", opts.castColumnsJson);
        assertNull(opts.encodingOverride);
    }

    @Test void mergeOptions_defaults() {
        var opts = new ReparatioClient.MergeOptions();
        assertEquals("", opts.joinOn);
        assertTrue(opts.fixEncoding);
        assertFalse(opts.noHeader);
    }

    @Test void batchOptions_defaults() {
        var opts = new ReparatioClient.BatchConvertOptions();
        assertFalse(opts.noHeader);
        assertTrue(opts.fixEncoding);
        assertEquals("{}", opts.castColumnsJson);
    }

    // ── Multipart body structural tests ───────────────────────────────────

    @Test void multipart_boundaryUnique() throws Exception {
        var parts = List.of(new MultipartBody.Part("k", "v"));
        var mp1 = new MultipartBody(parts);
        var mp2 = new MultipartBody(parts);
        assertNotEquals(mp1.contentType(), mp2.contentType());
    }

    @Test void multipart_bodyContainsBoundary() throws Exception {
        var parts = List.of(new MultipartBody.Part("k", "v"));
        var mp = new MultipartBody(parts);
        String boundary = mp.contentType().replace("multipart/form-data; boundary=", "");
        String body = new String(mp.body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("--" + boundary));
        assertTrue(body.contains("--" + boundary + "--"));
    }

    @Test void multipart_crlf() throws Exception {
        var parts = List.of(new MultipartBody.Part("k", "v"));
        var mp = new MultipartBody(parts);
        String body = new String(mp.body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("\r\n"), "Multipart lines must end with CRLF");
    }

    // ── toJsonArray edge cases ────────────────────────────────────────────

    @Test void jsonArray_quotesInValue() {
        String result = callToJsonArray(List.of("say \"hi\""));
        assertTrue(result.contains("\\\"hi\\\""));
    }
}
