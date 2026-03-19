import { describe, expect, it, vi } from "vitest";
import { S3Backend } from "./index";

function callResolvePath(backend: S3Backend, key: unknown): string {
	return (backend as unknown as { resolvePath: (path: string) => string }).resolvePath(
		key as string
	);
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

describe("S3Backend.resolvePath", () => {
	describe("normalization", () => {
		it("resolves absolute path from root", () => {
			const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
			expect(callResolvePath(backend, "/a/b/c.txt")).toBe("/a/b/c.txt");
		});

		it("resolves relative path from root", () => {
			const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
			expect(callResolvePath(backend, "a/b/c.txt")).toBe("/a/b/c.txt");
		});

		it("joins key under configured rootPrefix", () => {
			const backend = new S3Backend({
				bucketName: "test-bucket",
				rootPrefix: "/app/data/",
			});
			expect(callResolvePath(backend, "/logs/2026/03.txt")).toBe(
				"/app/data/logs/2026/03.txt"
			);
		});

		it("normalizes duplicate separators and dot segments", () => {
			const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/base" });
			expect(callResolvePath(backend, "//a///./b//c/")).toBe("/base/a/b/c");
		});

		it("normalizes backslashes to forward slashes", () => {
			const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "base" });
			expect(callResolvePath(backend, "\\a\\b\\c.txt")).toBe("/base/a/b/c.txt");
		});

		it("allows names containing '..' when not a traversal segment", () => {
			const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
			expect(callResolvePath(backend, "/folder/file..txt")).toBe("/folder/file..txt");
		});
	});

	describe("security checks", () => {
		it("rejects traversal with parent segment in relative input", () => {
			const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
			expect(() => callResolvePath(backend, "../secrets.txt")).toThrow(
				"Path traversal is not allowed"
			);
		});

		it("rejects traversal with parent segment in absolute input", () => {
			const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
			expect(() => callResolvePath(backend, "/safe/../secrets.txt")).toThrow(
				"Path traversal is not allowed"
			);
		});

		it("rejects traversal with backslash separated input", () => {
			const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
			expect(() => callResolvePath(backend, "safe\\..\\secrets.txt")).toThrow(
				"Path traversal is not allowed"
			);
		});

		it("rejects home-relative segment", () => {
			const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
			expect(() => callResolvePath(backend, "~")).toThrow(
				"Home-relative paths are not allowed"
			);
		});

		it("rejects home-prefixed segment", () => {
			const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
			expect(() => callResolvePath(backend, "/~user/secrets.txt")).toThrow(
				"Home-relative paths are not allowed"
			);
		});

		it("rejects null byte in input", () => {
			const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
			expect(() => callResolvePath(backend, "safe\0name.txt")).toThrow(
				"Path contains invalid characters"
			);
		});

		it("rejects non-string path values at runtime", () => {
			const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
			expect(() => callResolvePath(backend, 42)).toThrow("Path must be a string");
		});

		it("rejects unsafe rootPrefix when resolving", () => {
			const backend = new S3Backend({
				bucketName: "test-bucket",
				rootPrefix: "/safe/../escape",
			});

			expect(() => callResolvePath(backend, "file.txt")).toThrow(
				"Invalid rootPrefix; path traversal is not allowed"
			);
		});
	});
});

describe("S3Backend.lsInfo", () => {
	it("includes only direct child files and directories for a path", async () => {
		const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });

		const send = vi.fn().mockResolvedValue({
			Contents: [
				{ Key: "users/a.txt", Size: 10, LastModified: new Date("2026-03-10T00:00:00.000Z") },
				{ Key: "users/profile/avatar.png", Size: 11, LastModified: new Date("2026-03-10T00:00:00.000Z") },
				{ Key: "users-archive/b.txt", Size: 12, LastModified: new Date("2026-03-10T00:00:00.000Z") },
			],
			CommonPrefixes: [{ Prefix: "users/docs/" }],
			IsTruncated: false,
		});

		(backend as unknown as { s3Client: { send: typeof send } }).s3Client = { send };

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
		const firstCallInput = send.mock.calls[0][0].input as { Prefix?: string; Delimiter?: string };
		expect(firstCallInput.Prefix).toBe("users/");
		expect(firstCallInput.Delimiter).toBe("/");
	});

	it("uses continuation tokens for truncated list responses", async () => {
		const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });

		const send = vi
			.fn()
			.mockResolvedValueOnce({
				Contents: [{ Key: "users/a.txt", Size: 1, LastModified: new Date("2026-03-11T00:00:00.000Z") }],
				CommonPrefixes: [],
				IsTruncated: true,
				NextContinuationToken: "token-1",
			})
			.mockResolvedValueOnce({
				Contents: [{ Key: "users/b.txt", Size: 2, LastModified: new Date("2026-03-12T00:00:00.000Z") }],
				CommonPrefixes: [{ Prefix: "users/docs/" }],
				IsTruncated: false,
			});

		(backend as unknown as { s3Client: { send: typeof send } }).s3Client = { send };

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

		const firstCallInput = send.mock.calls[0][0].input as { ContinuationToken?: string };
		const secondCallInput = send.mock.calls[1][0].input as { ContinuationToken?: string };

		expect(firstCallInput.ContinuationToken).toBeUndefined();
		expect(secondCallInput.ContinuationToken).toBe("token-1");
	});
});

describe("S3Backend.dangerouslyListAllObjects", () => {
	it("summarizes first-page metadata and accumulates Contents/CommonPrefixes", async () => {
		const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });

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

		(backend as unknown as { s3Client: { send: typeof send } }).s3Client = { send };

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
			CommonPrefixes: [
				{ Prefix: "users/docs/" },
				{ Prefix: "users/images/" },
			],
		});

		const firstCallInput = send.mock.calls[0][0].input as { ContinuationToken?: string };
		const secondCallInput = send.mock.calls[1][0].input as { ContinuationToken?: string };

		expect(firstCallInput.ContinuationToken).toBeUndefined();
		expect(secondCallInput.ContinuationToken).toBe("token-1");
	});
});

describe("S3Backend.read", () => {
	it("formats selected lines with 1-based line numbers", async () => {
		const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
		const readRaw = vi
			.spyOn(backend, "readRaw")
			.mockResolvedValue({
				content: ["first", "second", "third", "fourth"],
				created_at: "",
				modified_at: "",
			});

		const result = await backend.read("/notes.txt", 1, 2);

		expect(result).toBe("2: second\n3: third");
		expect(readRaw).toHaveBeenCalledWith("/notes.txt");
	});

	it("uses default offset and limit", async () => {
		const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
		vi.spyOn(backend, "readRaw").mockResolvedValue({
			content: ["alpha", "beta"],
			created_at: "",
			modified_at: "",
		});

		const result = await backend.read("a.txt");

		expect(result).toBe("1: alpha\n2: beta");
	});

	it("returns empty string when offset is beyond available lines", async () => {
		const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
		vi.spyOn(backend, "readRaw").mockResolvedValue({
			content: ["only line"],
			created_at: "",
			modified_at: "",
		});

		const result = await backend.read("/a.txt", 5, 10);

		expect(result).toBe("");
	});

	it("rethrows errors from readRaw", async () => {
		const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
		vi.spyOn(backend, "readRaw").mockRejectedValue(new Error("s3 read failed"));

		await expect(backend.read("/a.txt")).rejects.toThrow("s3 read failed");
	});
});

describe("S3Backend.readRaw", () => {
	it("reads content from stream chunks and prefers metadata CreatedAt", async () => {
		const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/base" });
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

		(backend as unknown as { s3Client: { send: typeof send } }).s3Client = { send };

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
		const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
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

		(backend as unknown as { s3Client: { send: typeof send } }).s3Client = { send };

		const result = await backend.readRaw("emoji.txt");

		expect(result.content).toEqual(["A😊B"]);
	});

	it("returns empty content and empty timestamps when object body is missing", async () => {
		const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
		const send = vi
			.fn()
			.mockResolvedValueOnce({ Body: undefined })
			.mockResolvedValueOnce({ LastModified: new Date("2026-03-15T00:00:00.000Z") });

		(backend as unknown as { s3Client: { send: typeof send } }).s3Client = { send };

		const result = await backend.readRaw("/missing.txt");

		expect(result).toEqual({ content: [], created_at: "", modified_at: "" });
	});

	it("falls back to LastModified for created_at when metadata is absent", async () => {
		const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
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

		(backend as unknown as { s3Client: { send: typeof send } }).s3Client = { send };

		const result = await backend.readRaw("a.txt");

		expect(result).toEqual({
			content: ["one", "two"],
			created_at: "2026-03-16T07:08:09.000Z",
			modified_at: "2026-03-16T07:08:09.000Z",
		});
	});

	it("rethrows S3 errors", async () => {
		const backend = new S3Backend({ bucketName: "test-bucket", rootPrefix: "/" });
		const send = vi.fn().mockRejectedValue(new Error("network timeout"));
		(backend as unknown as { s3Client: { send: typeof send } }).s3Client = { send };

		await expect(backend.readRaw("a.txt")).rejects.toThrow("network timeout");
	});
});

