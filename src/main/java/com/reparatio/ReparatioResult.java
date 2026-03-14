package com.reparatio;

/**
 * Result of a file-producing API call (convert, batch-convert, merge, append, query).
 */
public final class ReparatioResult {
    private final byte[] content;
    private final String filename;
    private final String warning;

    public ReparatioResult(byte[] content, String filename, String warning) {
        this.content  = content;
        this.filename = filename;
        this.warning  = warning;
    }

    /** Raw bytes of the converted / queried output file. */
    public byte[] content()  { return content; }

    /** Suggested output filename from the Content-Disposition header. */
    public String filename() { return filename; }

    /**
     * Warning message from X-Reparatio-Warning or X-Reparatio-Errors, or null.
     */
    public String warning()  { return warning; }
}
