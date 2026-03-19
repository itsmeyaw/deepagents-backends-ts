import { S3Client, ListObjectsV2Command, ListObjectsV2CommandInput, GetObjectCommand, HeadObjectCommand, _Object, ListObjectsV2CommandOutput } from "@aws-sdk/client-s3";
import { EditResult, FileData, FileDownloadResponse, FileInfo, FileUploadResponse, GrepMatch, WriteResult, BackendProtocol } from "deepagents";

export class S3Backend implements BackendProtocol {
    private s3Client: S3Client;
    protected bucketName: string;
    protected cwd: string;
    private maxFileSizeBytes: number | undefined;

    constructor(options: 
        { 
            s3ClientConfig?: ConstructorParameters<typeof S3Client>,
            bucketName?: string,
            rootPrefix?: string,
            maxFileSizeMb?: number
        } = {}
    ) {
        this.s3Client = new S3Client(options?.s3ClientConfig || {});
        this.cwd = options?.rootPrefix || "/";
        this.maxFileSizeBytes = options?.maxFileSizeMb ? options?.maxFileSizeMb * 1024 * 1024 : undefined;

        if (!options?.bucketName) {
            throw new Error("bucketName is required in options");
        } else {
            this.bucketName = options.bucketName;
        }
    }

    /**
     * Resolve a path with security checks. This behaves similarly to the original 
     * `resolvePath` in the `FilesystemBackend`, but it always act in virtual mode.
     * 
     * @param key - The S3 object key to resolve. It should be an absolute path starting with /, but if not, it will be treated as relative to the root.
     * @returns Resolve the key to a normalized S3 object key. 
     * @throws Error if the key contains path traversal patterns or is invalid.
     */
    private resolvePath(key: string): string {
        if (typeof key !== "string") {
            throw new Error("Path must be a string");
        }

        const normalizedInput = key.replace(/\\/g, "/");
        if (normalizedInput.includes("\0")) {
            throw new Error("Path contains invalid characters");
        }

        const rootSegments = this.cwd
            .replace(/\\/g, "/")
            .split("/")
            .filter(Boolean);

        for (const segment of rootSegments) {
            if (segment === "." || segment === "..") {
                throw new Error("Invalid rootPrefix; path traversal is not allowed");
            }
        }

        const inputSegments = normalizedInput
            .split("/")
            .filter((segment) => segment.length > 0 && segment !== ".");

        for (const segment of inputSegments) {
            if (segment === "..") {
                throw new Error("Path traversal is not allowed");
            }

            if (segment === "~" || segment.startsWith("~")) {
                throw new Error("Home-relative paths are not allowed");
            }

            if (segment.includes("\0")) {
                throw new Error("Path contains invalid characters");
            }
        }

        const fullPathSegments = [...rootSegments, ...inputSegments];
        return `/${fullPathSegments.join("/")}`;
    }

    /**
     * List files and directories in the specified directory (non-recursive).
     *
     * @param dirPath - Absolute directory path to list files from
     * @returns List of FileInfo objects for files and directories directly in the directory.
     *          Directories have a trailing / in their path and is_dir=true.
     */
    async lsInfo(dirPath: string): Promise<FileInfo[]> {
        try {
            const resolvedPath = this.resolvePath(dirPath);
            const listPrefix = resolvedPath === "/" ? "" : `${resolvedPath.slice(1).replace(/\/+$/, "")}/`;
            const result = await this.dangerouslyListAllObjects({
                Bucket: this.bucketName,
                Prefix: listPrefix,
                Delimiter: "/",
            });

            const directFiles = (result.Contents || [])
                .filter((obj) => {
                    const key = obj.Key || "";
                    if (!key.startsWith(listPrefix)) {
                        return false;
                    }

                    const relativeKey = key.slice(listPrefix.length);
                    return relativeKey.length > 0 && !relativeKey.includes("/");
                })
                .map((obj) => ({
                    path: `/${obj.Key}`,
                    is_dir: false,
                    size: obj.Size,
                    modified_at: obj.LastModified?.getTime()
                } as FileInfo));

            const directDirectories = (result.CommonPrefixes || [])
                .map((prefix) => prefix.Prefix || "")
                .filter((prefix) => {
                    if (!prefix.startsWith(listPrefix)) {
                        return false;
                    }

                    const relativePrefix = prefix
                        .slice(listPrefix.length)
                        .replace(/\/+$/, "");

                    return relativePrefix.length > 0 && !relativePrefix.includes("/");
                })
                .map((prefix) => ({
                    path: `/${prefix}`,
                    is_dir: true,
                    size: undefined,
                    modified_at: undefined
                } as FileInfo));

            return [...directFiles, ...directDirectories];
        } catch {
            return [];
        }
    }

    /**
     * Read file content with line numbers.
     *
     * @param filePath - Absolute or relative file path
     * @param offset - Line offset to start reading from (0-indexed)
     * @param limit - Maximum number of lines to read
     * @returns Formatted file content with line numbers, or error message
     */
    async read(filePath: string, offset: number = 0, limit: number = 500): Promise<string> {
        try {
            const fileData = await this.readRaw(filePath);
            const selectedLines = fileData.content.slice(offset, offset + limit);
            return selectedLines.map((line, index) => `${offset + index + 1}: ${line}`).join("\n");
        } catch (error) {
            throw error;
        }
    }

    /**
     * Read file content as raw FileData.
     *
     * @param filePath - Absolute file path
     * @returns Raw file content as FileData
     */
    async readRaw(filePath: string): Promise<FileData> {
        try {
            const resolvedPath = this.resolvePath(filePath);

            const [getObjectResult, headObjectResult] = await Promise.all([
                this.s3Client.send(new GetObjectCommand({
                    Bucket: this.bucketName,
                    Key: resolvedPath.slice(1)
                })),
                this.s3Client.send(new HeadObjectCommand({
                    Bucket: this.bucketName,
                    Key: resolvedPath.slice(1)
                }))
            ]);

            if (!getObjectResult.Body) {
                return { content: [], created_at: "", modified_at: "" };
            }

            const headInfo = {
                size: headObjectResult.ContentLength,
                created_at: headObjectResult.Metadata?.CreatedAt || headObjectResult.LastModified?.toISOString() || "",
                modified_at: headObjectResult.LastModified?.toISOString() || ""
            };

            const stream = getObjectResult.Body as ReadableStream;
            const reader = stream.getReader();
            const decoder = new TextDecoder("utf-8");
            let content = "";
            let done = false;

            while (!done) {
                const { value, done: streamDone } = await reader.read();
                if (value) {
                    content += decoder.decode(value, { stream: true });
                }
                done = streamDone;
            }

            content += decoder.decode();

            const lines = content.split(/\r?\n/);

            return {
                content: lines,
                created_at: headInfo.created_at,
                modified_at: headInfo.modified_at,
            }
        } catch (error) {
            throw error;
        }
    }

    /**
     * Search for a literal text pattern in files (recursive).
     *
     * @param pattern - Literal string to search for (NOT regex).
     * @param dirPath - Directory or file path to search in. Defaults to current directory.
     * @param glob - Optional glob pattern to filter which files to search.
     * @returns List of GrepMatch dicts containing path, line number, and matched text.
     */
    async grepRaw(pattern: string, dirPath?: string | null, glob?: string | null): Promise<GrepMatch[] | string> {
        throw new Error("Method not implemented.");
    }

    globInfo(pattern: string, path?: string): Promise<FileInfo[]> {
        throw new Error("Method not implemented.");
    }
    write(filePath: string, content: string): Promise<WriteResult> {
        throw new Error("Method not implemented.");
    }
    edit(filePath: string, oldString: string, newString: string, replaceAll?: boolean): Promise<EditResult> {
        throw new Error("Method not implemented.");
    }
    uploadFiles?(files: Array<[string, Uint8Array]>): Promise<FileUploadResponse[]> {
        throw new Error("Method not implemented.");
    }
    downloadFiles?(paths: string[]): Promise<FileDownloadResponse[]> {
        throw new Error("Method not implemented.");
    }

    async dangerouslyListAllObjects(param: Omit<ListObjectsV2CommandInput, "ContinuationToken">): Promise<Pick<ListObjectsV2CommandOutput, "Name" | "Contents" | "CommonPrefixes" | "Prefix" | "Delimiter" | "MaxKeys">> {
        let isTruncated = true;
        let continuationToken: string | undefined;

        const allContents: _Object[] = [];
        const allCommonPrefixes: NonNullable<ListObjectsV2CommandOutput["CommonPrefixes"]> = [];

        let name: ListObjectsV2CommandOutput["Name"];
        let prefix: ListObjectsV2CommandOutput["Prefix"];
        let delimiter: ListObjectsV2CommandOutput["Delimiter"];
        let maxKeys: ListObjectsV2CommandOutput["MaxKeys"];

        while (isTruncated) {
            const result = await this.s3Client.send(new ListObjectsV2Command({
                ...param,
                ContinuationToken: continuationToken,
            }));

            if (name === undefined && result.Name !== undefined) {
                name = result.Name;
            }

            if (prefix === undefined && result.Prefix !== undefined) {
                prefix = result.Prefix;
            }

            if (delimiter === undefined && result.Delimiter !== undefined) {
                delimiter = result.Delimiter;
            }

            if (maxKeys === undefined && result.MaxKeys !== undefined) {
                maxKeys = result.MaxKeys;
            }

            if (result.Contents?.length) {
                allContents.push(...result.Contents);
            }

            if (result.CommonPrefixes?.length) {
                allCommonPrefixes.push(...result.CommonPrefixes);
            }

            isTruncated = result.IsTruncated === true;
            continuationToken = result.NextContinuationToken;
        }

        return {
            Name: name,
            Prefix: prefix ?? param.Prefix,
            Delimiter: delimiter ?? param.Delimiter,
            MaxKeys: maxKeys ?? param.MaxKeys,
            Contents: allContents,
            CommonPrefixes: allCommonPrefixes,
        };
    }
}