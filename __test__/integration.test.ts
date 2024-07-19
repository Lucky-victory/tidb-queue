import { TiQueue, Job, QueueOptions } from "../src/index";
import * as mysql from "mysql2/promise";
import dotenv from "dotenv";
dotenv.config();
describe("TiQueue Integration", () => {
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
    });

    // await queue["init"]();
  });

  afterAll(async () => {
    await queue.shutdown();
    await pool.end();
  },10000);

  beforeEach(async () => {
    await pool.query("DELETE FROM jobs");
  });

  test("add and process a job", async () => {
    const handler = jest.fn().mockResolvedValue("result");
    queue.handler(handler);

    const jobId = await queue.add({ data: "test" });

    // queue.start();

    // Wait for job to be processed
    await new Promise((resolve) => setTimeout(resolve, 2000));

    const job = await queue.getJobStatus(jobId);
console.log(job, " Integration 1");
    expect(job?.status).toBe("completed");
    expect(handler).toHaveBeenCalledWith(
      expect.objectContaining({ id: jobId })
    );
  });

  test("handles job failure and retry", async () => {
    let attempts = 0;
    const handler = jest.fn().mockImplementation(async () => {
      if (attempts++ < 1) throw new Error("Test error");
      return "success";
    });

    queue.handler(handler);

    const jobId = await queue.add(
      { data: "test" },
      { retryOnFail: true, maxAttempts: 3 }
    );

    // queue.start();

    // Wait for job to be processed and retried
    await new Promise((resolve) => setTimeout(resolve, 4000));

    const job = await queue.getJobStatus(jobId);
console.log(job,' Integration 2');
    expect(job?.status).toBe("completed");
    expect(job?.attempts).toBe(2);
    expect(handler).toHaveBeenCalledTimes(2);
  });
});
