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
    systemPrompt: `You are an assistant that helps users manage files in an S3 bucket. 
    Your job is to test all the capabilities of the S3 backend by performing file operations such as listing files, creating files, 
    reading files, and searching within files. Always use the provided tools for any file operations and 
    never attempt to perform file operations without them. Always provide clear and concise responses.`,
  });

  for await (const [namespace, chunk] of await deepAgent.stream(
    {
      messages: [
        {
          role: "user",
          content: `List all files in current directory. You are now writing a book stories. Come up with a title and make it a directory.
          Afterward, write each chapter as a separate file in that directory. Then read the content of the first chapter and search for a specific word in all chapters.
          Try to use all file capabiltiies provided, including grep, download, and upload.`,
        },
      ],
    },
    { streamMode: "messages", subgraphs: true },
  )) {
    const [message] = chunk;

    // Identify source: "main" or the subagent namespace segment
    const isSubagent = namespace.some((s: string) => s.startsWith("tools:"));
    const source = isSubagent
      ? namespace.find((s: string) => s.startsWith("tools:"))!
      : "main";

    // Tool call chunks (streaming tool invocations)
    if (
      AIMessageChunk.isInstance(message) &&
      message.tool_call_chunks?.length
    ) {
      for (const tc of message.tool_call_chunks) {
        if (tc.name) {
          console.log(`\n[${source}] Tool call: ${tc.name}`);
        }
        // Args stream in chunks - write them incrementally
        if (tc.args) {
          process.stdout.write(tc.args);
        }
      }
    }

    // Tool results
    if (ToolMessage.isInstance(message)) {
      console.log(
        `\n[${source}] Tool result [${message.name}]: ${message.text?.slice(0, 150)}`,
      );
    }

    // Regular AI content (skip tool call messages)
    if (
      AIMessageChunk.isInstance(message) &&
      message.text &&
      !message.tool_call_chunks?.length
    ) {
      process.stdout.write(message.text);
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
