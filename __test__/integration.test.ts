import dotenv from "dotenv";
dotenv.config();
import { TiQueue } from "../src/index";
import * as mysql from "mysql2/promise";

jest.setTimeout(30000); // Increase global timeout to 30 seconds

describe("TiQueue Integration Tests", () => {
  let queue: TiQueue;
  let pool: mysql.Pool;

  beforeAll(async () => {
    // Set up the database connection
    pool = mysql.createPool({
      host: process.env.DB_HOST,
      user: process.env.DB_USER_NAME,
      password: process.env.DB_USER_PASS,
      database: process.env.DB_NAME,
      ssl: { rejectUnauthorized: true },
    });

    // Create a new TiQueue instance
    queue = new TiQueue("test_queue", pool, {
      pollingInterval: "1s",
      autoStart: false,
      concurrency: 5,
      logging: { enabled: true, level: "info" },
    });

    // Wait for queue initialization
    await queue.init();
    queue.start();
  });
  beforeEach(async () => {
    await queue.deleteAllJobs();
  });
  afterAll(async () => {
    // Clean up
    await queue.shutdown();
  }, 30000);

  it("should add and process a job successfully", async () => {
    let jobCompleted = false;

    queue.handler(async (job) => {
      expect(job.payload).toEqual({ test: "data" });
      return "Job done";
    });

    queue.on("job_complete", (job, result) => {
      expect(result).toBe("Job done");
      jobCompleted = true;
    });

    const jobId = await queue.add({ test: "data" });

    // Wait for job to complete
    await new Promise<void>((resolve) => {
      const checkInterval = setInterval(() => {
        if (jobCompleted) {
          clearInterval(checkInterval);
          resolve();
        }
      }, 100);
    });

    const jobStatus = await queue.getJobStatus(jobId);
    expect(jobStatus?.status).toBe("completed");
  }, 15000);

  it("should handle job failures and retries", async () => {
    let attempts = 0;
    let jobFailed = false;

    queue.handler(async (job) => {
      attempts++;
      if (attempts < 3) {
        throw new Error("Job failed");
      }
      return "Job succeeded after retries";
    });

    queue.on("job_fail", (job, error) => {
      expect(error.message).toBe("Job failed");
      jobFailed = true;
    });

    const jobId = await queue.add({}, { maxAttempts: 3 });

    // Wait for job to fail
    await new Promise<void>((resolve) => {
      const checkInterval = setInterval(() => {
        if (jobFailed) {
          clearInterval(checkInterval);
          resolve();
        }
      }, 100);
    });

    const jobStatus = await queue.getJobStatus(jobId);
    expect(jobStatus?.status).toBe("failed");
    expect(attempts).toBe(3);
  }, 15000);

  it("should handle concurrent jobs correctly", async () => {
    const completedJobs: number[] = [];

    queue.on("job_complete", async (job) => {
      await new Promise((resolve) => setTimeout(resolve, 300)); // Simulate work
      completedJobs.push(job.id);
    });

    const jobIds = await Promise.all([
      queue.add({}),
      queue.add({}),
      queue.add({}),
      queue.add({}),
    ]);

    // Wait for all jobs to complete
    await new Promise<void>((resolve) => {
      const checkInterval = setInterval(() => {
        if (completedJobs.length === 4) {
          clearInterval(checkInterval);
          resolve();
        }
      }, 100);
    });

    expect(completedJobs).toHaveLength(4);
    expect(completedJobs).toEqual(expect.arrayContaining(jobIds));
  }, 25000);
});
