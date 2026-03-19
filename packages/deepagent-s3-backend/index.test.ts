import { describe, expect, it, vi } from "vitest";
import { S3Backend } from "./index";

function callResolvePath(backend: S3Backend, key: unknown): string {
  return (
    backend as unknown as { resolvePath: (path: string) => string }
  ).resolvePath(key as string);
}

function callMatchesGlob(
  backend: S3Backend,
  key: string,
  prefix: string,
  globPattern: string,
): boolean {
  return (
    backend as unknown as {
      matchesGlob: (
        key: string,
        prefix: string,
        globPattern: string,
      ) => boolean;
    }
  ).matchesGlob(key, prefix, globPattern);
}

function streamFromChunks(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(chunk);
      }

      controller.close();
    },
  });
}

function bodyWithTransformToString(content: string): {
  transformToString: (encoding?: string) => Promise<string>;
} {
  return {
    transformToString: vi.fn().mockResolvedValue(content),
  };
}

function bodyWithTransformToByteArray(content: Uint8Array): {
  transformToByteArray: () => Promise<Uint8Array>;
} {
  return {
    transformToByteArray: vi.fn().mockResolvedValue(content),
  };
}

describe("S3Backend.resolvePath", () => {
  describe("normalization", () => {
    it("resolves absolute path from root", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "/",
      });
      expect(callResolvePath(backend, "/a/b/c.txt")).toBe("/a/b/c.txt");
    });

    it("resolves relative path from root", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "/",
      });
      expect(callResolvePath(backend, "a/b/c.txt")).toBe("/a/b/c.txt");
    });

    it("joins key under configured rootPrefix", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "/app/data/",
      });
      expect(callResolvePath(backend, "/logs/2026/03.txt")).toBe(
        "/app/data/logs/2026/03.txt",
      );
    });

    it("normalizes duplicate separators and dot segments", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "/base",
      });
      expect(callResolvePath(backend, "//a///./b//c/")).toBe("/base/a/b/c");
    });

    it("normalizes backslashes to forward slashes", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "base",
      });
      expect(callResolvePath(backend, "\\a\\b\\c.txt")).toBe("/base/a/b/c.txt");
    });

    it("allows names containing '..' when not a traversal segment", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "/",
      });
      expect(callResolvePath(backend, "/folder/file..txt")).toBe(
        "/folder/file..txt",
      );
    });
  });

  describe("security checks", () => {
    it("rejects traversal with parent segment in relative input", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "/",
      });
      expect(() => callResolvePath(backend, "../secrets.txt")).toThrow(
        "Path traversal is not allowed",
      );
    });

    it("rejects traversal with parent segment in absolute input", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "/",
      });
      expect(() => callResolvePath(backend, "/safe/../secrets.txt")).toThrow(
        "Path traversal is not allowed",
      );
    });

    it("rejects traversal with backslash separated input", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "/",
      });
      expect(() => callResolvePath(backend, "safe\\..\\secrets.txt")).toThrow(
        "Path traversal is not allowed",
      );
    });

    it("rejects home-relative segment", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "/",
      });
      expect(() => callResolvePath(backend, "~")).toThrow(
        "Home-relative paths are not allowed",
      );
    });

    it("rejects home-prefixed segment", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "/",
      });
      expect(() => callResolvePath(backend, "/~user/secrets.txt")).toThrow(
        "Home-relative paths are not allowed",
      );
    });

    it("rejects null byte in input", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "/",
      });
      expect(() => callResolvePath(backend, "safe\0name.txt")).toThrow(
        "Path contains invalid characters",
      );
    });

    it("rejects non-string path values at runtime", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "/",
      });
      expect(() => callResolvePath(backend, 42)).toThrow(
        "Path must be a string",
      );
    });

    it("rejects unsafe rootPrefix when resolving", () => {
      const backend = new S3Backend({
        bucketName: "test-bucket",
        rootPrefix: "/safe/../escape",
      });

      expect(() => callResolvePath(backend, "file.txt")).toThrow(
        "Invalid rootPrefix; path traversal is not allowed",
      );
    });
  });
});

describe("S3Backend.lsInfo", () => {
  it("includes only direct child files and directories for a path", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });

    const send = vi.fn().mockResolvedValue({
      Contents: [
        {
          Key: "users/a.txt",
          Size: 10,
          LastModified: new Date("2026-03-10T00:00:00.000Z"),
        },
        {
          Key: "users/profile/avatar.png",
          Size: 11,
          LastModified: new Date("2026-03-10T00:00:00.000Z"),
        },
        {
          Key: "users-archive/b.txt",
          Size: 12,
          LastModified: new Date("2026-03-10T00:00:00.000Z"),
        },
      ],
      CommonPrefixes: [{ Prefix: "users/docs/" }],
      IsTruncated: false,
    });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.lsInfo("/users");

    expect(result).toEqual([
      {
        path: "/users/a.txt",
        is_dir: false,
        size: 10,
        modified_at: new Date("2026-03-10T00:00:00.000Z").getTime(),
      },
      {
        path: "/users/docs/",
        is_dir: true,
        size: undefined,
        modified_at: undefined,
      },
    ]);

    expect(send).toHaveBeenCalledTimes(1);
    const firstCallInput = send.mock.calls[0][0].input as {
      Prefix?: string;
      Delimiter?: string;
    };
    expect(firstCallInput.Prefix).toBe("users/");
    expect(firstCallInput.Delimiter).toBe("/");
  });

  it("uses continuation tokens for truncated list responses", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });

    const send = vi
      .fn()
      .mockResolvedValueOnce({
        Contents: [
          {
            Key: "users/a.txt",
            Size: 1,
            LastModified: new Date("2026-03-11T00:00:00.000Z"),
          },
        ],
        CommonPrefixes: [],
        IsTruncated: true,
        NextContinuationToken: "token-1",
      })
      .mockResolvedValueOnce({
        Contents: [
          {
            Key: "users/b.txt",
            Size: 2,
            LastModified: new Date("2026-03-12T00:00:00.000Z"),
          },
        ],
        CommonPrefixes: [{ Prefix: "users/docs/" }],
        IsTruncated: false,
      });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.lsInfo("/users");

    expect(result).toEqual([
      {
        path: "/users/a.txt",
        is_dir: false,
        size: 1,
        modified_at: new Date("2026-03-11T00:00:00.000Z").getTime(),
      },
      {
        path: "/users/b.txt",
        is_dir: false,
        size: 2,
        modified_at: new Date("2026-03-12T00:00:00.000Z").getTime(),
      },
      {
        path: "/users/docs/",
        is_dir: true,
        size: undefined,
        modified_at: undefined,
      },
    ]);

    const firstCallInput = send.mock.calls[0][0].input as {
      ContinuationToken?: string;
    };
    const secondCallInput = send.mock.calls[1][0].input as {
      ContinuationToken?: string;
    };

    expect(firstCallInput.ContinuationToken).toBeUndefined();
    expect(secondCallInput.ContinuationToken).toBe("token-1");
  });
});

describe("S3Backend.dangerouslyListAllObjects", () => {
  it("summarizes first-page metadata and accumulates Contents/CommonPrefixes", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });

    const send = vi
      .fn()
      .mockResolvedValueOnce({
        Name: "bucket-page-1",
        Prefix: "users/",
        Delimiter: "/",
        MaxKeys: 1000,
        Contents: [{ Key: "users/a.txt", Size: 1 }],
        CommonPrefixes: [{ Prefix: "users/docs/" }],
        IsTruncated: true,
        NextContinuationToken: "token-1",
      })
      .mockResolvedValueOnce({
        Name: "bucket-page-2",
        Prefix: "users/",
        Delimiter: "/",
        MaxKeys: 500,
        Contents: [{ Key: "users/b.txt", Size: 2 }],
        CommonPrefixes: [{ Prefix: "users/images/" }],
        IsTruncated: false,
      });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.dangerouslyListAllObjects({
      Bucket: "test-bucket",
      Prefix: "users/",
      Delimiter: "/",
      MaxKeys: 1000,
    });

    expect(result).toEqual({
      Name: "bucket-page-1",
      Prefix: "users/",
      Delimiter: "/",
      MaxKeys: 1000,
      Contents: [
        { Key: "users/a.txt", Size: 1 },
        { Key: "users/b.txt", Size: 2 },
      ],
      CommonPrefixes: [{ Prefix: "users/docs/" }, { Prefix: "users/images/" }],
    });

    const firstCallInput = send.mock.calls[0][0].input as {
      ContinuationToken?: string;
    };
    const secondCallInput = send.mock.calls[1][0].input as {
      ContinuationToken?: string;
    };

    expect(firstCallInput.ContinuationToken).toBeUndefined();
    expect(secondCallInput.ContinuationToken).toBe("token-1");
  });
});

describe("S3Backend.read", () => {
  it("formats selected lines with 1-based line numbers", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const readRaw = vi.spyOn(backend, "readRaw").mockResolvedValue({
      content: ["first", "second", "third", "fourth"],
      created_at: "",
      modified_at: "",
    });

    const result = await backend.read("/notes.txt", 1, 2);

    expect(result).toBe("2: second\n3: third");
    expect(readRaw).toHaveBeenCalledWith("/notes.txt");
  });

  it("uses default offset and limit", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    vi.spyOn(backend, "readRaw").mockResolvedValue({
      content: ["alpha", "beta"],
      created_at: "",
      modified_at: "",
    });

    const result = await backend.read("a.txt");

    expect(result).toBe("1: alpha\n2: beta");
  });

  it("returns empty string when offset is beyond available lines", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    vi.spyOn(backend, "readRaw").mockResolvedValue({
      content: ["only line"],
      created_at: "",
      modified_at: "",
    });

    const result = await backend.read("/a.txt", 5, 10);

    expect(result).toBe("");
  });

  it("rethrows errors from readRaw", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    vi.spyOn(backend, "readRaw").mockRejectedValue(new Error("s3 read failed"));

    await expect(backend.read("/a.txt")).rejects.toThrow("s3 read failed");
  });
});

describe("S3Backend.readRaw", () => {
  it("reads content from stream chunks and prefers metadata CreatedAt", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/base",
    });
    const lastModified = new Date("2026-03-13T10:11:12.000Z");
    const encoder = new TextEncoder();
    const text = "line1\nline2\nline3";
    const bytes = encoder.encode(text);

    const send = vi
      .fn()
      .mockResolvedValueOnce({
        Body: streamFromChunks([bytes.slice(0, 6), bytes.slice(6)]),
      })
      .mockResolvedValueOnce({
        ContentLength: bytes.length,
        Metadata: { CreatedAt: "2026-01-01T00:00:00.000Z" },
        LastModified: lastModified,
      });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.readRaw("logs/app.log");

    expect(result).toEqual({
      content: ["line1", "line2", "line3"],
      created_at: "2026-01-01T00:00:00.000Z",
      modified_at: "2026-03-13T10:11:12.000Z",
    });

    expect(send).toHaveBeenCalledTimes(2);
    expect(send.mock.calls[0][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "base/logs/app.log",
    });
    expect(send.mock.calls[1][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "base/logs/app.log",
    });
  });

  it("decodes UTF-8 correctly when a multi-byte character is split across chunks", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const encoder = new TextEncoder();
    const bytes = encoder.encode("A😊B");

    const send = vi
      .fn()
      .mockResolvedValueOnce({
        Body: streamFromChunks([bytes.slice(0, 3), bytes.slice(3)]),
      })
      .mockResolvedValueOnce({
        Metadata: {},
        LastModified: new Date("2026-03-14T00:00:00.000Z"),
      });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.readRaw("emoji.txt");

    expect(result.content).toEqual(["A😊B"]);
  });

  it("returns empty content and empty timestamps when object body is missing", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi
      .fn()
      .mockResolvedValueOnce({ Body: undefined })
      .mockResolvedValueOnce({
        LastModified: new Date("2026-03-15T00:00:00.000Z"),
      });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.readRaw("/missing.txt");

    expect(result).toEqual({ content: [], created_at: "", modified_at: "" });
  });

  it("falls back to LastModified for created_at when metadata is absent", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const lastModified = new Date("2026-03-16T07:08:09.000Z");
    const send = vi
      .fn()
      .mockResolvedValueOnce({
        Body: streamFromChunks([new TextEncoder().encode("one\ntwo")]),
      })
      .mockResolvedValueOnce({
        Metadata: undefined,
        LastModified: lastModified,
      });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.readRaw("a.txt");

    expect(result).toEqual({
      content: ["one", "two"],
      created_at: "2026-03-16T07:08:09.000Z",
      modified_at: "2026-03-16T07:08:09.000Z",
    });
  });

  it("rethrows S3 errors", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi.fn().mockRejectedValue(new Error("network timeout"));
    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    await expect(backend.readRaw("a.txt")).rejects.toThrow("network timeout");
  });
});

describe("S3Backend.grepRaw", () => {
  it("lists objects by prefix and returns literal line matches", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi
      .fn()
      .mockResolvedValueOnce({
        Contents: [
          { Key: "logs/a.txt", Size: 20 },
          { Key: "logs/b.txt", Size: 30 },
        ],
        CommonPrefixes: [],
        IsTruncated: false,
      })
      .mockResolvedValueOnce({
        Body: bodyWithTransformToString("line1\nneedle one"),
      })
      .mockResolvedValueOnce({
        Body: bodyWithTransformToString("needle two\nline2\nneedle three"),
      });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.grepRaw("needle", "/logs");

    expect(result).toEqual([
      { path: "/logs/a.txt", line: 2, text: "needle one" },
      { path: "/logs/b.txt", line: 1, text: "needle two" },
      { path: "/logs/b.txt", line: 3, text: "needle three" },
    ]);

    const listInput = send.mock.calls[0][0].input as { Prefix?: string };
    expect(listInput.Prefix).toBe("logs");
  });

  it("applies glob filtering before reading objects", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi
      .fn()
      .mockResolvedValueOnce({
        Contents: [
          { Key: "logs/app.log", Size: 20 },
          { Key: "logs/readme.md", Size: 30 },
          { Key: "logs/errors/trace.log", Size: 40 },
        ],
        CommonPrefixes: [],
        IsTruncated: false,
      })
      .mockResolvedValueOnce({
        Body: bodyWithTransformToString("start\nneedle"),
      })
      .mockResolvedValueOnce({
        Body: bodyWithTransformToString("needle in nested log"),
      });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.grepRaw("needle", "/logs", "*.log");

    expect(result).toEqual([
      { path: "/logs/app.log", line: 2, text: "needle" },
      { path: "/logs/errors/trace.log", line: 1, text: "needle in nested log" },
    ]);
    expect(send).toHaveBeenCalledTimes(3);
  });

  it("continues searching when one object read fails", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi
      .fn()
      .mockResolvedValueOnce({
        Contents: [
          { Key: "logs/ok.txt", Size: 20 },
          { Key: "logs/broken.txt", Size: 30 },
        ],
        CommonPrefixes: [],
        IsTruncated: false,
      })
      .mockResolvedValueOnce({
        Body: bodyWithTransformToString("needle survives"),
      })
      .mockRejectedValueOnce(new Error("S3 GetObject failed"));

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.grepRaw("needle", "/logs");

    expect(result).toEqual([
      { path: "/logs/ok.txt", line: 1, text: "needle survives" },
    ]);
  });
});

describe("S3Backend.globInfo", () => {
  it("returns matching files as FileInfo under the provided base path", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi.fn().mockResolvedValueOnce({
      Contents: [
        {
          Key: "logs/app.log",
          Size: 20,
          LastModified: new Date("2026-03-17T00:00:00.000Z"),
        },
        {
          Key: "logs/readme.md",
          Size: 30,
          LastModified: new Date("2026-03-17T00:00:00.000Z"),
        },
        {
          Key: "logs/errors/trace.log",
          Size: 40,
          LastModified: new Date("2026-03-18T00:00:00.000Z"),
        },
        {
          Key: "logs/folder/",
          Size: 0,
          LastModified: new Date("2026-03-18T00:00:00.000Z"),
        },
      ],
      CommonPrefixes: [],
      IsTruncated: false,
    });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.globInfo("*.log", "/logs");

    expect(result).toEqual([
      {
        path: "/logs/app.log",
        is_dir: false,
        size: 20,
        modified_at: new Date("2026-03-17T00:00:00.000Z").getTime(),
      },
      {
        path: "/logs/errors/trace.log",
        is_dir: false,
        size: 40,
        modified_at: new Date("2026-03-18T00:00:00.000Z").getTime(),
      },
    ]);

    const listInput = send.mock.calls[0][0].input as { Prefix?: string };
    expect(listInput.Prefix).toBe("logs");
  });

  it("uses virtual root by default and respects configured rootPrefix", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/base",
    });
    const send = vi.fn().mockResolvedValueOnce({
      Contents: [
        {
          Key: "base/a.ts",
          Size: 1,
          LastModified: new Date("2026-03-18T12:00:00.000Z"),
        },
        {
          Key: "base/docs/readme.md",
          Size: 2,
          LastModified: new Date("2026-03-18T12:00:00.000Z"),
        },
      ],
      CommonPrefixes: [],
      IsTruncated: false,
    });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.globInfo("**/*.ts");

    expect(result).toEqual([
      {
        path: "/base/a.ts",
        is_dir: false,
        size: 1,
        modified_at: new Date("2026-03-18T12:00:00.000Z").getTime(),
      },
    ]);

    const listInput = send.mock.calls[0][0].input as { Prefix?: string };
    expect(listInput.Prefix).toBe("base");
  });
});

describe("S3Backend.matchesGlob", () => {
  it("matches relative path under prefix with recursive glob", () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });

    expect(
      callMatchesGlob(backend, "logs/errors/trace.log", "logs", "**/*.log"),
    ).toBe(true);
  });

  it("matches basename-only patterns", () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });

    expect(
      callMatchesGlob(backend, "logs/errors/trace.log", "logs", "trace.*"),
    ).toBe(true);
    expect(
      callMatchesGlob(backend, "logs/errors/trace.log", "logs", "*.log"),
    ).toBe(true);
  });

  it("matches full-key anchored patterns", () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });

    expect(
      callMatchesGlob(
        backend,
        "logs/errors/trace.log",
        "other",
        "logs/errors/*.log",
      ),
    ).toBe(true);
  });

  it("supports single-character wildcard (?)", () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });

    expect(callMatchesGlob(backend, "logs/ab.log", "logs", "?.log")).toBe(
      false,
    );
    expect(callMatchesGlob(backend, "logs/a.log", "logs", "?.log")).toBe(true);
  });

  it("matches dotfiles when dot option is enabled", () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });

    expect(callMatchesGlob(backend, "logs/.env", "logs", "*.env")).toBe(true);
    expect(
      callMatchesGlob(
        backend,
        "logs/.github/workflows/ci.yml",
        "logs",
        "**/*.yml",
      ),
    ).toBe(true);
  });

  it("returns false when no target variant matches", () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });

    expect(
      callMatchesGlob(backend, "logs/errors/trace.log", "logs", "*.md"),
    ).toBe(false);
    expect(
      callMatchesGlob(
        backend,
        "logs/errors/trace.log",
        "logs",
        "reports/**/*.log",
      ),
    ).toBe(false);
  });

  it("treats trailing slash in prefix as valid for relative matching", () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });

    expect(
      callMatchesGlob(backend, "logs/errors/trace.log", "logs/", "**/*.log"),
    ).toBe(true);
  });

  it("does not crash when prefix is empty", () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });

    expect(
      callMatchesGlob(backend, "logs/errors/trace.log", "", "logs/**/*.log"),
    ).toBe(true);
    expect(callMatchesGlob(backend, "logs/errors/trace.log", "", "*.txt")).toBe(
      false,
    );
  });
});

describe("S3Backend.write", () => {
  it("creates a new object when the target key does not already exist", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/base",
    });
    const send = vi
      .fn()
      .mockResolvedValueOnce({ Body: undefined })
      .mockResolvedValueOnce({});

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-19T12:34:56.789Z"));

    const result = await backend.write("notes/today.txt", "hello from test");

    expect(result).toEqual({
      path: "/base/notes/today.txt",
      filesUpdate: null,
    });

    expect(send).toHaveBeenCalledTimes(2);
    expect(send.mock.calls[0][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "base/notes/today.txt",
    });
    expect(send.mock.calls[1][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "base/notes/today.txt",
      Body: "hello from test",
      Metadata: {
        CreatedAt: "2026-03-19T12:34:56.789Z",
      },
    });

    vi.useRealTimers();
  });

  it("creates a new object when GetObject returns a null Body", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi
      .fn()
      .mockResolvedValueOnce({ Body: null })
      .mockResolvedValueOnce({});

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.write("/docs/new.md", "new content");

    expect(result).toEqual({
      path: "/docs/new.md",
      filesUpdate: null,
    });
    expect(send).toHaveBeenCalledTimes(2);
    expect(send.mock.calls[0][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "docs/new.md",
    });
    expect(send.mock.calls[1][0].input).toMatchObject({
      Bucket: "test-bucket",
      Key: "docs/new.md",
      Body: "new content",
    });
  });

  it("returns an already-exists error and skips PutObject when Body is present", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi
      .fn()
      .mockResolvedValueOnce({ Body: bodyWithTransformToString("existing") });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.write("/docs/readme.md", "new content");

    expect(result).toEqual({
      error: "File already exists at path: /docs/readme.md",
    });
    expect(send).toHaveBeenCalledTimes(1);
    expect(send.mock.calls[0][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "docs/readme.md",
    });
  });

  it("returns the S3 error message when GetObject fails", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi.fn().mockRejectedValueOnce(new Error("NoSuchKey"));

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.write("/missing.txt", "content");

    expect(result).toEqual({ error: "NoSuchKey" });
    expect(send).toHaveBeenCalledTimes(1);
  });

  it("returns the S3 error message when PutObject fails", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi
      .fn()
      .mockResolvedValueOnce({ Body: undefined })
      .mockRejectedValueOnce(new Error("AccessDenied"));

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.write("/docs/restricted.txt", "content");

    expect(result).toEqual({ error: "AccessDenied" });
    expect(send).toHaveBeenCalledTimes(2);
  });

  it("returns path validation errors without calling S3", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi.fn();

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.write("../secrets.txt", "content");

    expect(result).toEqual({ error: "Path traversal is not allowed" });
    expect(send).not.toHaveBeenCalled();
  });

  it("falls back to unknown error message for non-Error throwables", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi.fn().mockRejectedValueOnce({});

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.write("/docs/file.txt", "content");

    expect(result).toEqual({ error: "Unknown error during write operation" });
    expect(send).toHaveBeenCalledTimes(1);
  });
});

describe("S3Backend.edit", () => {
  it("replaces only the first occurrence by default", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/base",
    });
    const readRaw = vi.spyOn(backend, "readRaw").mockResolvedValue({
      content: ["alpha target", "beta target", "target gamma"],
      created_at: "2026-03-20T00:00:00.000Z",
      modified_at: "2026-03-20T00:00:00.000Z",
    });
    const send = vi.fn().mockResolvedValue({});

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.edit("notes/today.txt", "target", "REPLACED");

    expect(result).toEqual({
      path: "/base/notes/today.txt",
      filesUpdate: null,
      occurrences: 1,
    });
    expect(readRaw).toHaveBeenCalledWith("/base/notes/today.txt");
    expect(send).toHaveBeenCalledTimes(1);
    expect(send.mock.calls[0][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "base/notes/today.txt",
      Body: "alpha REPLACED\nbeta target\ntarget gamma",
      Metadata: {
        CreatedAt: "2026-03-20T00:00:00.000Z",
      },
    });
  });

  it("replaces all occurrences when replaceAll is true and reports total replacements", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    vi.spyOn(backend, "readRaw").mockResolvedValue({
      content: ["target a", "b target", "target c target"],
      created_at: "2026-03-21T00:00:00.000Z",
      modified_at: "2026-03-21T00:00:00.000Z",
    });
    const send = vi.fn().mockResolvedValue({});

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.edit("/logs/app.log", "target", "X", true);

    expect(result).toEqual({
      path: "/logs/app.log",
      filesUpdate: null,
      occurrences: 4,
    });
    expect(send).toHaveBeenCalledTimes(1);
    expect(send.mock.calls[0][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "logs/app.log",
      Body: "X a\nb X\nX c X",
      Metadata: {
        CreatedAt: "2026-03-21T00:00:00.000Z",
      },
    });
  });

  it("returns not-found error when oldString does not exist and skips PutObject", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    vi.spyOn(backend, "readRaw").mockResolvedValue({
      content: ["alpha", "beta"],
      created_at: "2026-03-21T00:00:00.000Z",
      modified_at: "2026-03-21T00:00:00.000Z",
    });
    const send = vi.fn();

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.edit("/notes.txt", "target", "X");

    expect(result).toEqual({
      error: 'The string "target" was not found in the file.',
    });
    expect(send).not.toHaveBeenCalled();
  });

  it("uses current time for CreatedAt when original created_at is empty", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    vi.spyOn(backend, "readRaw").mockResolvedValue({
      content: ["replace me"],
      created_at: "",
      modified_at: "2026-03-22T00:00:00.000Z",
    });
    const send = vi.fn().mockResolvedValue({});

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-22T12:00:00.000Z"));

    const result = await backend.edit("/notes.txt", "replace", "updated");

    expect(result).toEqual({
      path: "/notes.txt",
      filesUpdate: null,
      occurrences: 1,
    });
    expect(send.mock.calls[0][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "notes.txt",
      Body: "updated me",
      Metadata: {
        CreatedAt: "2026-03-22T12:00:00.000Z",
      },
    });

    vi.useRealTimers();
  });

  it("returns read errors and skips PutObject", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    vi.spyOn(backend, "readRaw").mockRejectedValue(new Error("NoSuchKey"));
    const send = vi.fn();

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.edit("/missing.txt", "a", "b");

    expect(result).toEqual({ error: "NoSuchKey" });
    expect(send).not.toHaveBeenCalled();
  });

  it("returns put errors when writing updated content fails", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    vi.spyOn(backend, "readRaw").mockResolvedValue({
      content: ["replace me"],
      created_at: "2026-03-23T00:00:00.000Z",
      modified_at: "2026-03-23T00:00:00.000Z",
    });
    const send = vi.fn().mockRejectedValue(new Error("AccessDenied"));

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.edit("/docs/file.txt", "replace", "updated");

    expect(result).toEqual({ error: "AccessDenied" });
    expect(send).toHaveBeenCalledTimes(1);
  });

  it("returns path validation errors before reading file", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const readRaw = vi.spyOn(backend, "readRaw");
    const send = vi.fn();

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.edit("../secrets.txt", "a", "b");

    expect(result).toEqual({ error: "Path traversal is not allowed" });
    expect(readRaw).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();
  });

  it("falls back to unknown error message for non-Error throwables", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    vi.spyOn(backend, "readRaw").mockResolvedValue({
      content: ["replace me"],
      created_at: "2026-03-23T00:00:00.000Z",
      modified_at: "2026-03-23T00:00:00.000Z",
    });
    const send = vi.fn().mockRejectedValue({});

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.edit("/docs/file.txt", "replace", "updated");

    expect(result).toEqual({
      error: "Unknown error during edit operation",
    });
  });
});

describe("S3Backend.uploadFiles", () => {
  it("uploads multiple files and returns successful responses", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/base",
    });
    const send = vi.fn().mockResolvedValue({});
    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-24T09:10:11.123Z"));

    const first = new Uint8Array([1, 2, 3]);
    const second = new Uint8Array([4, 5]);
    const result = await backend.uploadFiles([
      ["/docs/a.txt", first],
      ["images/b.png", second],
    ]);

    expect(result).toEqual(
      expect.arrayContaining([
        { path: "/base/docs/a.txt", error: null },
        { path: "/base/images/b.png", error: null },
      ]),
    );
    expect(result).toHaveLength(2);

    expect(send).toHaveBeenCalledTimes(2);
    expect(send.mock.calls[0][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "base/docs/a.txt",
      Body: first,
      Metadata: {
        CreatedAt: "2026-03-24T09:10:11.123Z",
      },
    });
    expect(send.mock.calls[1][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "base/images/b.png",
      Body: second,
      Metadata: {
        CreatedAt: "2026-03-24T09:10:11.123Z",
      },
    });

    vi.useRealTimers();
  });

  it("returns one response per input when uploads complete out of order", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });

    let resolveFirst: (() => void) | undefined;
    const firstPromise = new Promise((resolve) => {
      resolveFirst = () => resolve(undefined);
    });

    const send = vi
      .fn()
      .mockImplementationOnce(() => firstPromise)
      .mockResolvedValueOnce({});

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const uploadPromise = backend.uploadFiles([
      ["/slow.txt", new Uint8Array([1])],
      ["/fast.txt", new Uint8Array([2])],
    ]);

    await Promise.resolve();
    resolveFirst?.();

    const result = await uploadPromise;

    expect(result).toEqual(
      expect.arrayContaining([
        { path: "/slow.txt", error: null },
        { path: "/fast.txt", error: null },
      ]),
    );
    expect(result).toHaveLength(2);
    expect(send).toHaveBeenCalledTimes(2);
  });

  it("keeps processing other files when one upload throws", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi
      .fn()
      .mockResolvedValueOnce({})
      .mockRejectedValueOnce(new Error("AccessDenied"));

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.uploadFiles([
      ["/ok.txt", new Uint8Array([1])],
      ["/denied.txt", new Uint8Array([2])],
    ]);

    expect(result).toEqual(
      expect.arrayContaining([
        { path: "/ok.txt", error: null },
        { path: "/denied.txt", error: null },
      ]),
    );
    expect(result).toHaveLength(2);
    expect(send).toHaveBeenCalledTimes(2);
  });

  it("reports an entry for invalid paths and skips S3 call for those entries", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi.fn().mockResolvedValue({});
    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.uploadFiles([
      ["../blocked.txt", new Uint8Array([9])],
      ["/safe.txt", new Uint8Array([8])],
    ]);

    expect(result).toEqual(
      expect.arrayContaining([
        { path: "../blocked.txt", error: null },
        { path: "/safe.txt", error: null },
      ]),
    );
    expect(result).toHaveLength(2);
    expect(send).toHaveBeenCalledTimes(1);
    expect(send.mock.calls[0][0].input).toMatchObject({
      Bucket: "test-bucket",
      Key: "safe.txt",
    });
  });

  it("returns empty results for empty input without calling S3", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi.fn();
    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.uploadFiles([]);

    expect(result).toEqual([]);
    expect(send).not.toHaveBeenCalled();
  });
});

describe("S3Backend.downloadFiles", () => {
  it("downloads multiple files and returns byte content", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/base",
    });
    const first = new Uint8Array([1, 2, 3]);
    const second = new Uint8Array([9, 8]);

    const send = vi
      .fn()
      .mockResolvedValueOnce({ Body: bodyWithTransformToByteArray(first) })
      .mockResolvedValueOnce({ Body: bodyWithTransformToByteArray(second) });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.downloadFiles(["/docs/a.txt", "images/b.bin"]);

    expect(result).toEqual([
      { path: "/docs/a.txt", content: first, error: null },
      { path: "images/b.bin", content: second, error: null },
    ]);

    expect(send).toHaveBeenCalledTimes(2);
    expect(send.mock.calls[0][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "base/docs/a.txt",
    });
    expect(send.mock.calls[1][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "base/images/b.bin",
    });
  });

  it("maps missing objects to file_not_found", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi.fn().mockRejectedValue({ name: "NoSuchKey" });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.downloadFiles(["/missing.txt"]);

    expect(result).toEqual([
      { path: "/missing.txt", content: null, error: "file_not_found" },
    ]);
  });

  it("maps access errors to permission_denied", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const send = vi.fn().mockRejectedValue({ code: "AccessDenied" });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.downloadFiles(["/protected.txt"]);

    expect(result).toEqual([
      { path: "/protected.txt", content: null, error: "permission_denied" },
    ]);
  });

  it("returns invalid_path for unsafe path input and continues with other files", async () => {
    const backend = new S3Backend({
      bucketName: "test-bucket",
      rootPrefix: "/",
    });
    const safeContent = new Uint8Array([7]);
    const send = vi
      .fn()
      .mockResolvedValue({ Body: bodyWithTransformToByteArray(safeContent) });

    (backend as unknown as { s3Client: { send: typeof send } }).s3Client = {
      send,
    };

    const result = await backend.downloadFiles(["../blocked.txt", "/safe.txt"]);

    expect(result).toEqual([
      { path: "../blocked.txt", content: null, error: "invalid_path" },
      { path: "/safe.txt", content: safeContent, error: null },
    ]);

    expect(send).toHaveBeenCalledTimes(1);
    expect(send.mock.calls[0][0].input).toEqual({
      Bucket: "test-bucket",
      Key: "safe.txt",
    });
  });
});
