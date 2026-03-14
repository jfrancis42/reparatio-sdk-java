package com.reparatio;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * Builds a multipart/form-data request body manually — no external libraries needed.
 */
final class MultipartBody {

    static final class Part {
        final String name;
        final String textValue;  // non-null for text fields
        final byte[] bytes;      // non-null for file/binary fields
        final String filename;
        final String contentType;

        /** Text field. */
        Part(String name, String value) {
            this.name        = name;
            this.textValue   = value;
            this.bytes       = null;
            this.filename    = null;
            this.contentType = null;
        }

        /** Binary / file field. */
        Part(String name, byte[] bytes, String filename, String contentType) {
            this.name        = name;
            this.textValue   = null;
            this.bytes       = bytes;
            this.filename    = filename;
            this.contentType = contentType;
        }

        boolean isFile() { return bytes != null; }
    }

    private final String boundary;
    private final byte[] body;

    MultipartBody(List<Part> parts) throws IOException {
        this.boundary = "ReparatioBoundary" + UUID.randomUUID().toString().replace("-", "");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Part p : parts) {
            writeln(out, "--" + boundary);
            if (p.isFile()) {
                writeln(out, "Content-Disposition: form-data; name=\"" + p.name
                        + "\"; filename=\"" + p.filename + "\"");
                writeln(out, "Content-Type: " + p.contentType);
                writeln(out, "");
                out.write(p.bytes);
                writeln(out, "");
            } else {
                writeln(out, "Content-Disposition: form-data; name=\"" + p.name + "\"");
                writeln(out, "");
                out.write(p.textValue.getBytes(StandardCharsets.UTF_8));
                writeln(out, "");
            }
        }
        writeln(out, "--" + boundary + "--");
        this.body = out.toByteArray();
    }

    private static void writeln(ByteArrayOutputStream out, String s) throws IOException {
        out.write(s.getBytes(StandardCharsets.US_ASCII));
        out.write('\r');
        out.write('\n');
    }

    String contentType() {
        return "multipart/form-data; boundary=" + boundary;
    }

    byte[] body() { return body; }
}
