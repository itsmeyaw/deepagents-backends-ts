# DeepAgent S3 Backend (TypeScript)

A TypeScript implementation of S3 Backend. Currently, it is using the Backend Protocol V1.

## Installation

```bash
npm add deepagent-s3-backend
```

or

```bash
pnpm add deepagent-s3-backend
```

## Usage

Apart from the client configuration for AWS S3 SDK, you need to pass the bucket
name. Additionally, you can pass the root prefix and maximum file size (in Megabytes).
