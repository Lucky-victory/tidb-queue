import { TiQueue } from "../src/index";
import * as mysql from "mysql2/promise";

jest.mock("mysql2/promise");

describe("TiQueue", () => {
  let tiQueue: TiQueue;
  let mockPool: jest.Mocked<mysql.Pool>;

  beforeEach(() => {
    mockPool = {
      query: jest.fn(),
      getConnection: jest.fn(),
      end: jest.fn(),
    } as any;

    (mysql.createPool as jest.Mock).mockReturnValue(mockPool);

    tiQueue = new TiQueue("testQueue", {}, { autoStart: false });
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  test("should initialize the queue", async () => {
    await tiQueue["initPromise"];
    expect(mockPool.query).toHaveBeenCalledTimes(2);
  });

  test("should add a job to the queue", async () => {
    const payload = { data: "test" };
    //@ts-ignore
    mockPool.query.mockResolvedValueOnce([{ insertId: 1 }]);

    const jobId = await tiQueue.add(payload);

    expect(jobId).toBe(1);
    expect(mockPool.query).toHaveBeenCalledWith(
      expect.stringContaining("INSERT INTO jobs"),
      expect.arrayContaining([
        "testQueue",
        expect.any(String),
        JSON.stringify(payload),
        expect.any(Date),
        0,
        3,
        "5s",
        false,
        3,
        undefined,
      ])
    );
  });

  test("should process jobs", async () => {
    const mockJob = { id: 1, jobId: "test-job", payload: "{}" };
    mockPool.getConnection.mockResolvedValue({
      beginTransaction: jest.fn(),
      query: jest.fn().mockResolvedValueOnce([[mockJob]]),
      commit: jest.fn(),
      release: jest.fn(),
    } as any);

    tiQueue.handler(async () => {});
    await tiQueue["processJobs"]();

    expect(mockPool.getConnection).toHaveBeenCalled();
    expect(tiQueue["activeJobs"]).toBe(1);
  });

  test("should handle job completion", async () => {
    const mockJob = { id: 1, jobId: "test-job", payload: "{}" };
    await tiQueue["completeJob"](mockJob as any, {});

    expect(mockPool.query).toHaveBeenCalledWith(
      'UPDATE jobs SET status = "completed" WHERE id = ?',
      [1]
    );
  });
});
