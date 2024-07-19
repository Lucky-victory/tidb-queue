import { TiQueue, Job, QueueOptions } from "../src/index";
import * as mysql from "mysql2/promise";
import dotenv from "dotenv";
dotenv.config();
describe("TiQueue Performance", () => {
  let queue: TiQueue;
  let pool: mysql.Pool;

  beforeAll(async () => {
    pool = mysql.createPool({
      host: process.env.DB_HOST,
      user: process.env.DB_USER_NAME,
      password: process.env.DB_USER_PASS,
      port: parseInt(process.env.DB_PORT + ""),
      ssl: { rejectUnauthorized: true },
      database: process.env.DB_NAME,
      connectionLimit: 10,
    });

    queue = new TiQueue("testQueue", pool as any, {
      pollingInterval: "100ms",
      concurrency: 10,
    });

    // await queue["init"]();
  });

  afterAll(async () => {
    await queue.shutdown();
    await pool.end();
  }, 60000);

  beforeEach(async () => {
    await pool.query("DELETE FROM jobs");
  });

  test("processes 1000 jobs under 30 seconds", async () => {
    const numJobs = 1000;
    const handler = jest.fn().mockResolvedValue("result");
    queue.handler(handler);

    // Add jobs
    for (let i = 0; i < numJobs; i++) {
      await queue.add({ data: `test${i}` });
    }

    const startTime = Date.now();
    // queue.start();

    // Wait for all jobs to be processed
    while (true) {
      const stats = await queue.getQueueStats();
      if (stats.completed === numJobs) break;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;

    console.log(`Processed ${numJobs} jobs in ${duration} seconds`);

    expect(duration).toBeLessThan(30);
    expect(handler).toHaveBeenCalledTimes(numJobs);
  }, 32000);

  test("handles high concurrency without errors", async () => {
    const numJobs = 500;
    const concurrentAdds = 50;
    const handler = jest.fn().mockResolvedValue("result");
    queue.handler(handler);

    // Add jobs concurrently
    const addPromises = [];
    for (let i = 0; i < numJobs; i++) {
      addPromises.push(queue.add({ data: `test${i}` }));
      if (addPromises.length === concurrentAdds) {
        await Promise.all(addPromises);
        addPromises.length = 0;
      }
    }
    if (addPromises.length > 0) {
      await Promise.all(addPromises);
    }

    // queue.start();

    // Wait for all jobs to be processed
    while (true) {
      const stats = await queue.getQueueStats();
      if (stats.completed === numJobs) break;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    const [failedJobs] = await pool.query(
      'SELECT COUNT(*) as count FROM jobs WHERE status = "failed"'
    );
    expect((failedJobs as mysql.RowDataPacket)[0].count).toBe(0);
    expect(handler).toHaveBeenCalledTimes(numJobs);
  }, 15000);
});
