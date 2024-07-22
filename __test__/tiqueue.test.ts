import { TiQueue } from "../src/index"; // Adjust the import path accordingly
import * as mysql from "mysql2/promise";
import dotenv from "dotenv";

dotenv.config();
let queue: TiQueue;
let pool: mysql.Pool;

beforeAll(async () => {
  pool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER_NAME,
    password: process.env.DB_USER_PASS,
    database: process.env.DB_NAME,
    port: parseInt(process.env.DB_PORT + ""),

    ssl: { rejectUnauthorized: true },
    connectionLimit: 10,
  });

  queue = new TiQueue("testQueue", pool as any, {
    pollingInterval: "1s",
    concurrency: 5,
    logging: { enabled: true },
  });

  queue = new TiQueue("testQueue", pool, { autoStart: false });
});
beforeEach(() => {
  console.log(
    {
      queueStatus: queue.queueStatus,
    },
    ".....0"
  );

  if (!queue.queueStatus.isRunning) queue.start();

  if (queue.queueStatus.isPaused) queue.resume();
});
afterEach(() => {
  queue.handler = () => {};
  queue.pause();
});
afterAll(async () => {
  await queue.shutdown();
  // await pool.end();
}, 15000);

describe("TiQueue Integration Tests", () => {
  test("should add and process a job successfully", async () => {
    let jobProcessed = false;
    const jobId = await queue.addJob({ key: "value" });
    queue.handler(async (job) => {
      jobProcessed = true;
      return "Job completed";
    });

    await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait for job to process

    queue.on("completed", async () => {
      const job = await queue.getJobStatus(jobId);
      expect(job?.status).toBe("completed");
      expect(jobProcessed).toBe(true);
    });
  }, 10000);

  test("should add multiple jobs", async () => {
   
  }, 20000);
});
