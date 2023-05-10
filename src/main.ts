import { S3, CompletedPart } from "@aws-sdk/client-s3";
import { readFileSync } from "fs";
import md5 from "md5";

const s3 = new S3({
  credentials: {
    secretAccessKey: "<AWS_SECRET>",
    accessKeyId: "<AWS_ID>",
  },
  region: "us-east-2",
});

export async function main() {
  const bucket = "mediabox.local.upload";
  const file = readFileSync("./1.mp4");
  const { length } = file;

  console.log({ length });

  let start = performance.now();
  await s3.putObject({ Key: 'test_single.mp4', Bucket: bucket, Body: file })
  console.log(`single: ${Math.ceil((performance.now() - start) / 1000)}s`)

  start = performance.now();
  await uploadFileInChunks(file, "test_parallel.mp4", bucket);
  console.log(`parallel: ${Math.ceil((performance.now() - start) / 1000)}s`);
}

function generateDigest(chunks: Buffer[]) {
  const fromHexString = (hexString: string) =>
    Uint8Array.from(Buffer.from(hexString, "hex"));
  return `${md5(fromHexString(chunks.map((chunk) => md5(chunk)).join("")))}-${
    chunks.length
  }`;
}

function chunkFile(file: Buffer, chunkSize = 1048576 * 5) {
  const totalChunks = Math.ceil(file.length / chunkSize);
  return Array.from(Array(totalChunks).keys()).map((index) => {
    const start = index * chunkSize;
    const end = start + chunkSize;
    return file.subarray(start, end);
  });
}

async function uploadFileInChunks(file: Buffer, Key: string, Bucket: string) {
  /* setup */
  const chunks = chunkFile(file);
  const digest = `"${generateDigest(chunks)}"`;
  const completedParts: CompletedPart[] = [];
  const sortByPartNumber = (
    { PartNumber: a }: CompletedPart,
    { PartNumber: b }: CompletedPart
  ) => (a ?? 0) - (b ?? 0);

  /* request multipart upload */
  const { UploadId } = await s3.createMultipartUpload({ Bucket, Key });

  if (!UploadId) throw new Error(`Multipart Upload Creation Failed for ${Key}`);

  /* upload parts */
  await Promise.all(
    chunks
      .map((Body, index) => {
        const PartNumber = index + 1;
        const localHash = `"${md5(Body)}"`;
        return async () => {
          const { ETag } = await s3.uploadPart({
            UploadId,
            Bucket,
            Key,
            PartNumber,
            Body,
          });

          if (localHash !== ETag) {
            console.warn("Hash mismatch");
            // TODO reupload chunk
          }

          console.log({ PartNumber });
          completedParts.push({ ETag, PartNumber });
        };
      })
      .map((chunk) => chunk())
  );

  const { Parts: serverParts } = await s3.listParts({ Bucket, Key, UploadId });

  if (serverParts?.length !== chunks.length) {
    console.warn("Different Parts on server");
    // TODO reupload missing parts
  }

  /* request completion */
  const { ETag } = await s3.completeMultipartUpload({
    Bucket,
    Key,
    UploadId,
    MultipartUpload: {
      Parts: completedParts.sort(sortByPartNumber),
    },
  });

  // TODO handle known error codes

  console.warn({ ETag, digest });
  if (ETag !== digest) {
    console.warn(`Hash mismatch for object`);
    // TODO handle error
  }
}

main();
