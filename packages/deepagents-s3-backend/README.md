# DeepAgents S3 Backend

A TypeScript implementation of S3 Backend. Currently, it is using the Backend Protocol V1.

## Installation

```bash
npm install deepagents-s3-backend
```

or

```bash
pnpm add deepagents-s3-backend
```

## Usage

Apart from the client configuration for AWS S3 SDK, you need to pass the bucket
name. Additionally, you can pass the root prefix and maximum file size (in Megabytes).

Example

```ts
import { ChatBedrockConverse } from "@langchain/aws";
import dotenv from "dotenv";
import { S3Backend } from "deepagents-s3-backend";
import { createDeepAgent } from "deepagents";
import { AIMessageChunk, ToolMessage } from "langchain";

dotenv.config();

const model = new ChatBedrockConverse({
  model: "mistral.devstral-2-123b",
  region: "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
});

const backend = new S3Backend({
  s3ClientConfig: {
    region: "us-east-1",
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
    },
  },
  bucketName: process.env.S3_BUCKET_NAME!,
  rootPrefix: "/root",
  maxFileSizeMb: 100,
});

async function main() {
  const deepAgent = await createDeepAgent({
    model,
    backend,
    name: "S3FileAgent",
    systemPrompt: `System message here`,
  });

  console.log(
    deepAgent.invoke({
      messages: [
        {
          role: "user",
          content: `User message`,
        },
      ],
    }),
  );
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
```
