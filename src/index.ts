import ms from "ms";
import { EventEmitter } from "events";
import * as mysql from "mysql2/promise";
import { fork, ChildProcess } from "child_process";
import { CronJob } from "cron";
import { v4 as uuid } from "uuid";
import isEmpty from "just-is-empty";
import { logger, Logger } from "./logger";
import { Job, JobOptions, QueueOptions } from "./types";

export class TiQueue extends EventEmitter {
  private pool: mysql.Pool;
  private pollingInterval: number;
  private concurrency: number;
  public isRunning: boolean = false;
  private _isRunning: boolean = false;
  private isPaused: boolean = false;
  private isShuttingDown: boolean = false;
  private activeJobs: number = 0;
  private cronJobs: Map<number, CronJob> = new Map();
  private workers: ChildProcess[] = [];
  private logger?: Logger;
  private useSeparateProcess: boolean;
  private workerFile?: string;
  private pollIntervalId?: NodeJS.Timeout;
  private queueName: string;
  private jobHandlers: Map<string, (job: Job) => Promise<any>> = new Map();
  private initPromise: Promise<void>;
  /**
   * Constructs a new instance of the `TiQueue` class.
   *
   * @param queueName - The name of the queue.
   * @param dbConfigOrPool - The MySQL database configuration or a pre-existing MySQL connection pool.
   * @param options - Optional configuration options for the queue.
   *   - `pollingInterval` - The interval at which the queue should poll for new jobs, in milliseconds.
   *   - `concurrency` - The maximum number of concurrent jobs the queue can process.
   *   - `useSeparateProcess` - Whether to use a separate process for job processing.
   *   - `workerFile` - The file path of the worker script to use for job processing.
   *   - `logging` - Configuration options for logging, including `enabled`, `level`, and `filename`.
   *   - `autoStart` - Whether to automatically start the queue when the instance is created.
   */
  constructor(
    queueName: string,
    dbConfigOrPool: mysql.PoolOptions | mysql.Pool,
    options: QueueOptions = {}
  ) {
    super();
    this.queueName = queueName;
    this.pool = this.isMySQLPool(dbConfigOrPool)
      ? (dbConfigOrPool as mysql.Pool)
      : mysql.createPool(dbConfigOrPool);
    this.pollingInterval = this.parseTime(options.pollingInterval || "5s");
    this.concurrency = options.concurrency || 5;
    this.useSeparateProcess = options.useSeparateProcess || false;
    this.workerFile = options.workerFile;

    if (options.logging && options.logging.enabled) {
      const loggerOpt = {
        queueName: this.queueName,
        level: options.logging.level,
        filename: options.logging.filename,
      };
      this.logger = logger(loggerOpt);
    }

    this.initPromise = (async () => await this.init())();

    if (options.autoStart) {
      this.initPromise.then(() => this.start());
    }

    // Set up signal handlers for graceful shutdown
    process.on("SIGINT", this.handleShutdownSignal.bind(this));
    process.on("SIGTERM", this.handleShutdownSignal.bind(this));
  }
  private set queueStatus({
    isRunning,
    isPaused,
  }: {
    isRunning: boolean;
    isPaused: boolean;
  }) {
    this.emit("statusChange", { isRunning, isPaused });
  }
  get queueStatus() {
    return { isRunning: this._isRunning, isPaused: this.isPaused };
  }
  private isMySQLPool(obj: any) {
    return (
      obj &&
      typeof obj === "object" &&
      typeof obj.getConnection === "function" &&
      typeof obj.query === "function" &&
      typeof obj.end === "function"
    );
  }
  private parseTime(time: string): number {
    return ms(time);
  }

  async init(): Promise<void> {
    try {
      await this.pool.query(`
      CREATE TABLE IF NOT EXISTS jobs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        queueName VARCHAR(50) NOT NULL,
        jobId CHAR(36) DEFAULT UUID(),
        payload JSON,
        status ENUM('queued', 'processing', 'completed', 'failed') DEFAULT 'queued',
        runAt TIMESTAMP NOT NULL,
        priority INT DEFAULT 0,
        attempts INT DEFAULT 0,
        maxAttempts INT DEFAULT 3,
        retryOnFail BOOLEAN DEFAULT FALSE,
        maxRetryOnFailAttempts INT DEFAULT 3,
        retryOnFailAttempts INT DEFAULT 0,
        retryOnFailInterval VARCHAR(10) DEFAULT '5s',
        cron VARCHAR(100),
        createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);
      await this.pool.query(
        "CREATE INDEX IF NOT EXISTS idx_queueName_status_runAt_priority_job_id ON jobs (queueName, status, runAt, priority, jobId)"
      );
      this.emit("queue_init");
    } catch (error) {
      this.log("error", "Error initializing queue: " + this.queueName);
      console.error("Error initializing the database:", error);
    }
  }

  async add(payload: any, options: JobOptions = {}): Promise<number> {
    await this.initPromise;
    const {
      delay = "0s",
      priority = 0,
      maxAttempts = 3,
      cron,
      jobId: customJobId,
      retryOnFailInterval = "5s",
      retryOnFail = false,
      maxRetryOnFailAttempts = 3,
    } = options;
    const runAt = new Date(Date.now() + this.parseTime(delay));
    const [result] = await this.pool.query<mysql.ResultSetHeader>(
      "INSERT INTO jobs (queueName, jobId, payload, runAt, priority, maxAttempts, retryOnFailInterval, retryOnFail,maxRetryOnFailAttempts, cron) VALUES (?, ?,?, ?, ?, ?, ?, ?, ?, ?)",
      [
        this.queueName,
        !isEmpty(customJobId) ? customJobId : uuid(),
        JSON.stringify(payload),
        runAt,
        priority,
        maxAttempts,
        retryOnFailInterval,
        retryOnFail,
        maxRetryOnFailAttempts,
        cron,
      ]
    );

    const jobId = result.insertId;

    if (cron) {
      this.setupCronJob(jobId, cron);
    }

    return jobId;
  }

  /**
   * Alias for add method
   */
  async addJob(payload: any, options: JobOptions = {}): Promise<number> {
    return this.add(payload, options);
  }

  handler(fn: (job: Job) => Promise<any>): void {
    if (this.jobHandlers.has(this.queueName)) {
      this.log(
        "warn",
        `Handler for queue ${this.queueName} is being overwritten`
      );
    }
    this.jobHandlers.set(this.queueName, fn);
  }
  start(): void {
    if (this._isRunning) return;
    this._isRunning = true;
    this.isPaused = false;
    if (this.pollIntervalId) clearInterval(this.pollIntervalId);
    this.pollIntervalId = setInterval(
      () => this.processJobs(),
      this.pollingInterval
    );

    this.emit("queue_start");
    this.log("info", "Job queue started");
  }

  stop(): void {
    if (!this._isRunning) return;
    this._isRunning = false;
    if (this.pollIntervalId) {
      clearInterval(this.pollIntervalId);
      this.pollIntervalId = undefined;
    }
    this.cronJobs.forEach((job) => job.stop());
    this.cronJobs.clear();
    this.workers.forEach((worker) => worker.kill());
    this.workers = [];
    this.emit("queue_stop");
    this.log("info", "Job queue stopped");
  }

  pause(): void {
    this.isPaused = true;
    this.emit("queue_pause");
    this.log("info", "Job queue paused");
  }

  resume(): void {
    this.isPaused = false;
    this.emit("queue_resume");
    this.log("info", "Job queue resumed");
  }

  private async processJobs(): Promise<void> {
    if (
      this.isPaused ||
      this.activeJobs >= this.concurrency ||
      !this._isRunning
    )
      return;

    const jobsToProcess = this.concurrency - this.activeJobs;
    if (jobsToProcess <= 0) return;

    const connection = await this.pool.getConnection();
    try {
      await connection.beginTransaction();

      const [_jobs] = await connection.query(
        `
        SELECT * FROM jobs 
        WHERE queueName = ? AND status = 'queued' AND runAt <= NOW()
        ORDER BY priority DESC, runAt
        LIMIT ?
        FOR UPDATE SKIP LOCKED
      `,
        [this.queueName, jobsToProcess]
      );
      const jobs = _jobs as Job[];
      for (const job of jobs) {
        await connection.query(
          'UPDATE jobs SET status = "processing", attempts = attempts + 1 WHERE id = ?',
          [job.id]
        );
      }

      await connection.commit();

      for (const job of jobs) {
        this.activeJobs++;
        this.emit("job_start", job);
        this.log(
          "info",
          `Starting job '${job.jobId}' in queue '${this.queueName}'`
        );
        await this.processJob(job);
      }
    } catch (error) {
      if (connection) {
        await connection.rollback();
      }
      this.log("error", "Error processing jobs:", error);
      this.emit("error", error);
    } finally {
      if (connection) {
        connection.release();
      }
    }
  }

  private async processJob(job: Job): Promise<void> {
    if (this.useSeparateProcess) {
      this.processJobInSeparateProcess(job);
    } else {
      await this.processJobInline(job);
    }
  }

  private async processJobInline(job: Job): Promise<void> {
    try {
      const handler = this.jobHandlers.get(this.queueName);
      if (!handler) {
        throw new Error(`No handler registered for queue '${this.queueName}'`);
      }
      const result = await handler(job);
      await this.completeJob(job, result);
    } catch (error) {
      await this.handleFailedJob(job, error as Error);
    } finally {
      this.activeJobs--;
    }
  }

  private processJobInSeparateProcess(job: Job): void {
    if (!this.workerFile) {
      throw new Error("Worker file path not specified");
    }
    const worker = fork(this.workerFile);
    this.workers.push(worker);

    const cleanupWorker = () => {
      const index = this.workers.indexOf(worker);
      if (index > -1) {
        this.workers.splice(index, 1);
      }
    };

    worker.on(
      "message",
      async (message: { status: string; result?: any; error?: Error }) => {
        try {
          if (message.status === "completed") {
            await this.completeJob(job, message.result);
          } else if (message.status === "failed") {
            await this.handleFailedJob(job, message?.error!);
          }
        } catch (error) {
          this.log(
            "error",
            `Error handling job ${job.jobId} completion:`,
            error
          );
          this.emit("error", error);
        } finally {
          this.activeJobs--;
          cleanupWorker();
          worker.kill();
        }
      }
    );

    worker.on("error", (error) => {
      this.log("error", `Worker error for job ${job.jobId}:`, error);
      this.emit("error", error);
      cleanupWorker();
    });

    worker.on("exit", (code) => {
      if (code !== 0) {
        this.log(
          "warn",
          `Worker for job ${job.jobId} exited with code ${code}`
        );
      }
      cleanupWorker();
    });

    worker.send(job);
  }

  private async completeJob(job: Job, result: any): Promise<void> {
    await this.pool.query('UPDATE jobs SET status = "completed" WHERE id = ?', [
      job.id,
    ]);
    job.status = "completed";
    this.log("info", `Job ${job.jobId} completed successfully`);
    this.emit("job_complete", job, result);
  }

  private async handleFailedJob(job: Job, error: Error): Promise<void> {
    try {
      console.log({ att: job.attempts, maxAtt: job.maxAttempts });

      if (job.attempts > job.maxAttempts) {
        if (
          job.retryOnFail &&
          job.retryOnFailAttempts < job.maxRetryOnFailAttempts
        ) {
          // Job has exhausted its initial attempts, but can still be retried
          const nextRetryAt = this.parseTime(job.retryOnFailInterval) / 1000;
          await this.pool.query(
            'UPDATE jobs SET status = "queued", attempts = 0, runAt = NOW() + INTERVAL ?, retryOnFailAttempts = retryOnFailAttempts + 1 WHERE id = ?',
            [nextRetryAt, job.id]
          );
          this.log(
            "info",
            `Job ${job.jobId} failed and scheduled for retry at ${ms(
              nextRetryAt
            )}`
          );
          this.emit("job_retry", job, error);
        } else {
          // Job has exhausted all attempts and retries
          await this.pool.query(
            'UPDATE jobs SET status = "failed" WHERE id = ?',
            [job.id]
          );
          this.log(
            "warn",
            `Job ${job.jobId} from '${job.queueName}' failed after ${job.attempts} attempts and ${job.retryOnFailAttempts} retries`
          );
          this.emit("job_fail", job, error);
        }
      } else {
        // Job has not exhausted its initial attempts, reschedule it
        await this.pool.query(
          'UPDATE jobs SET status = "queued", runAt = NOW() + INTERVAL ? SECOND, priority = priority + 1 WHERE id = ?',
          [this.parseTime(job.retryOnFailInterval) / 1000, job.id]
        );
        this.log("info", `Job ${job.jobId} failed and rescheduled for retry`);
        this.emit("job_retry", job, error);
      }
    } catch (err) {
      this.log("error", `Error handling failed job ${job.jobId}:`, err);
      this.emit("error", err);
    }
  }
  private async handleShutdownSignal() {
    this.log(
      "info",
      "Shutdown signal received. Initiating graceful shutdown..."
    );
    try {
      await this.shutdown();
      process.exit(0);
    } catch (error) {
      this.log("error", "Error during shutdown:", error);
      process.exit(1);
    }
  }
  async shutdown(timeout: number = 5000): Promise<void> {
    if (this.isShuttingDown) {
      this.log("warn", "Shutdown already in progress");
      return;
    }

    this.isShuttingDown = true;
    this.log("info", "Initiating shutdown process");

    // Stop accepting new jobs
    this.stop();

    // Wait for active jobs to complete (with timeout)
    const shutdownPromise = new Promise<void>((resolve) => {
      const checkActiveJobs = setInterval(() => {
        if (this.activeJobs === 0) {
          clearInterval(checkActiveJobs);
          resolve();
        }
      }, 100);
    });

    try {
      await Promise.race([
        shutdownPromise,
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("Shutdown timeout")), timeout)
        ),
      ]);

      // Gracefully shutdown workers
      await Promise.all(
        this.workers.map(
          async (worker) => await this.gracefullyShutdownWorker(worker)
        )
      );

      // Close database connection
      if (this.pool) await this.pool.end();

      this.log("info", "Queue shut down successfully");
    } catch (error) {
      this.log("error", "Error during shutdown:", error);
      // Force shutdown if timeout occurs
      this.workers.forEach((worker) => worker.kill());
      if (this.pool) await this.pool.end();
    }
  }

  private async gracefullyShutdownWorker(worker: ChildProcess): Promise<void> {
    return new Promise((resolve) => {
      worker.send("shutdown");
      worker.on("exit", () => {
        resolve();
      });
      // Fallback in case the worker doesn't exit on its own
      setTimeout(() => {
        worker.kill();
        resolve();
      }, 5000);
    });
  }

  async getJobStatus(jobId: number | string): Promise<Job | null> {
    const [_jobs] = await this.pool.query(
      "SELECT * FROM jobs WHERE id = ? OR jobId = ? AND queueName = ?",
      [jobId, jobId, this.queueName]
    );
    const jobs = _jobs as Job[];
    return jobs[0] || null;
  }

  async cancelJob(jobId: number | string): Promise<boolean> {
    const [result] = await this.pool.query<mysql.ResultSetHeader>(
      'UPDATE jobs SET status = "failed" WHERE (id = ? OR jobId = ?)  AND queueName = ? AND status IN ("queued", "processing")',
      [jobId, jobId, this.queueName]
    );
    this.emit("job_cancel", jobId);
    return result.affectedRows > 0;
  }

  async retryJob(jobId: number | string): Promise<boolean> {
    const [result] = await this.pool.query<mysql.ResultSetHeader>(
      'UPDATE jobs SET status = "queued", attempts = 0,retryOnFailAttempts = 0, runAt = NOW() WHERE (id = ? OR jobId = ?)  AND queueName = ? AND status = "failed"',
      [jobId, jobId, this.queueName]
    );
    this.emit("job_retry", jobId);
    return result.affectedRows > 0;
  }

  async getQueueStats(): Promise<{
    queued: number;
    processing: number;
    completed: number;
    failed: number;
  }> {
    const [_rows] = await this.pool.query(
      `
      SELECT status, COUNT(*) as count
      FROM jobs
      WHERE queueName = ?
      GROUP BY status
    `,
      [this.queueName]
    );
    const rows = _rows as {
      status: "queued" | "completed" | "processing" | "failed";
      count: number;
    }[];

    const stats = { queued: 0, processing: 0, completed: 0, failed: 0 };
    rows.forEach((row) => {
      stats[row.status as keyof typeof stats] = row.count;
    });

    return stats;
  }

  async cleanup(daysToKeep: number = 30): Promise<void> {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);

    await this.pool.query(
      `
      DELETE FROM jobs 
      WHERE queueName = ?
      AND (status = 'completed' OR status = 'failed') 
      AND updated_at < ?
      AND cron IS NULL
    `,
      [this.queueName, cutoffDate]
    );
  }
  async deleteAllJobs() {
    try {
      await this.pool.query("DELETE FROM jobs");
      this.log("info", "deleted all jobs");
    } catch (error) {
      this.log("info", "Something went wrong, couldn't delete jobs...");
    }
  }
  private log(level: string, message: string, meta?: any): void {
    if (this.logger) {
      this.logger.log(
        level,
        `Time:${new Date().toLocaleString()}: - ${message}`,
        meta
      );
    }
  }

  private setupCronJob(jobId: number, cronExpression: string): void {
    const cronJob = new CronJob(
      cronExpression,
      async () => await this.triggerCronJob(jobId)
    );
    this.cronJobs.set(jobId, cronJob);
    cronJob.start();
  }

  private async triggerCronJob(jobId: number): Promise<void> {
    await this.pool.query(
      'UPDATE jobs SET runAt = NOW(), attempts = 0,retryOnFailAttempts = 0, WHERE id = ? AND queueName = ? AND status != "processing"',
      [jobId, this.queueName]
    );
  }
}

export default TiQueue;
