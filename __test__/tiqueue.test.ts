import { TiQueue, Job, QueueOptions } from "../src/index";
import * as mysql from "mysql2/promise";
import { EventEmitter } from "events";
import { CronJob } from "cron";
import * as winston from "winston";

jest.mock("mysql2/promise");
jest.mock("cron");
jest.mock("winston");

describe("TiQueue", () => {
  let queue: TiQueue;
  let mockPool: jest.Mocked<mysql.Pool>;
  let mockConnection: jest.Mocked<mysql.PoolConnection>;

  beforeEach(() => {
    mockPool = {
      query: jest.fn(),
      getConnection: jest.fn(),
      end: jest.fn(),
    } as any;
    mockConnection = {
      query: jest.fn(),
      beginTransaction: jest.fn(),
      commit: jest.fn(),
      rollback: jest.fn(),
      release: jest.fn(),
    } as any;

    (mysql.createPool as jest.Mock).mockReturnValue(mockPool);
    mockPool.getConnection.mockResolvedValue(mockConnection);

    const queueOptions: QueueOptions = {
      pollingInterval: "5s",
      concurrency: 5,
      autoStart: false,
      useSeparateProcess: false,
    };

    queue = new TiQueue("testQueue", {}, queueOptions);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  test("constructor initializes properties correctly", () => {
    expect(queue).toBeInstanceOf(EventEmitter);
    expect(mysql.createPool).toHaveBeenCalled();
    expect(queue["pollingInterval"]).toBe(5000);
    expect(queue["concurrency"]).toBe(5);
  });

  test("add method inserts a job into the database", async () => {
    const payload = { data: "test" };
    const options = { priority: 1 };

    mockPool.query.mockResolvedValueOnce([{ insertId: 1 }] as any);

    const jobId = await queue.add(payload, options);

    expect(jobId).toBe(1);
    expect(mockPool.query).toHaveBeenCalledWith(
      expect.stringContaining("INSERT INTO jobs"),
      expect.arrayContaining([
        "testQueue",
        expect.any(String), // jobId
        JSON.stringify(payload),
        expect.any(Date), // runAt
        1, // priority
        3, // maxAttempts
        "1m", // retryInterval
        false, // retryOnFail
        undefined, // cron
      ])
    );
  });

  test("start method starts processing jobs", () => {
    const spy = jest.spyOn(global, "setInterval");
    queue.start();

    expect(queue["isRunning"]).toBe(true);
    expect(spy).toHaveBeenCalledWith(expect.any(Function), 5000);
    expect(queue["pollIntervalId"]).toBeDefined();

    spy.mockRestore();
  });

  test("stop method stops processing jobs", () => {
    queue["isRunning"] = true;
    queue["pollIntervalId"] = setTimeout(() => {}, 0);

    const spy = jest.spyOn(global, "clearInterval");
    queue.stop();

    expect(queue["isRunning"]).toBe(false);
    expect(spy).toHaveBeenCalledWith(queue["pollIntervalId"]);

    spy.mockRestore();
  });

  test("processJobs method processes queued jobs", async () => {
    const mockJob: Job = {
      id: 1,
      queueName: "testQueue",
      payload: { data: "test" },
      status: "queued",
      runAt: new Date(),
      priority: 0,
      attempts: 0,
      jobId: "123",
      delay: "0s",
      maxAttempts: 3,
      retryOnFail: false,
      retryOnFailInterval: "1m",
      maxRetryOnFailAttempts: 3,
      retryOnFailAttempts: 0,
      cron: "",
    };

    mockConnection.query.mockResolvedValueOnce([[mockJob]] as any);
    queue["jobHandlers"].set(
      "testQueue",
      jest.fn().mockResolvedValue("result")
    );

    await (queue as any).processJobs();

    expect(mockConnection.beginTransaction).toHaveBeenCalled();
    expect(mockConnection.query).toHaveBeenCalledWith(
      expect.stringContaining("SELECT * FROM jobs"),
      ["testQueue", 5]
    );
    expect(mockConnection.query).toHaveBeenCalledWith(
      expect.stringContaining('UPDATE jobs SET status = "processing"'),
      [1]
    );
    expect(mockConnection.commit).toHaveBeenCalled();
    expect(queue["jobHandlers"].get("testQueue")).toHaveBeenCalledWith(mockJob);
  });

  // Add more tests for other methods...
});
