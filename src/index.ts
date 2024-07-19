import ms from "ms";
import { EventEmitter } from "events";
import * as mysql from "mysql2/promise";
import { fork, ChildProcess } from "child_process";
import { CronJob } from "cron";
import * as winston from "winston";
import { v4 as uuid } from "uuid";
import isEmpty from "just-is-empty";

export interface Job<P = any> {
  id: number;
  queueName: string;
  payload: P;
  status: "queued" | "processing" | "completed" | "failed";
  runAt: Date;
  priority: number;
  attempts: number;
  jobId: string;
  delay: string;
  maxAttempts: number;
  retryOnFail: boolean;
  retryOnFailInterval: string;
  maxRetryOnFailAttempts: number;
  retryOnFailAttempts: number;
  cron: string;
}

export interface JobOptions {
  /**
   * Set a custom id for the job (not more than 36 characters), e.g uuid
   */
  jobId?: string;
  /**
   * The delay before the job starts in this format 1s, 2m, 1h
   */
  delay?: string;

  priority?: number;
  /**
   * The maximum number of attempts to run the job, default 3
   */
  maxAttempts?: number;
  /**
   * Whether to retry the job when it fails, default true
   */
  retryOnFail?: boolean;
  /**
   * The maximum number of retry attempts when the job fails, default is 3
   */
  maxRetryOnFailAttempts?: number;
  /**
   * The interval between retry attempts when the job fails, default is 1m
   */
  retryOnFailInterval?: string;

  /**
   * The cron expression for scheduling the job, e.g. "0 0 * * *" for every midnight
   */
  cron?: string;
}

export interface QueueOptions {
  /**
   * How frequently should the jobs be checked,  (default is 5s)
   */
  pollingInterval?: string;
  /**
   * How many concurrent jobs to run at once,(default is 5)
   */
  concurrency?: number;
  /**
   * Whether to automatically start the job queue, (default is true)
   */
  autoStart?: boolean;
  /**
   * Whether to use a separate process for running jobs, (default is false)
   */
  useSeparateProcess?: boolean;
  /**
   * The path to the worker file if useSeparateProcess is true
   * */
  workerFile?: string;
  /**
   * Whether to enable logging to the stdout and a file or not.
   */
  logging?: {
    enabled: boolean;

    level?: string;
    /**
     * Logs filename, default is job_queue.log
     */
    filename?: string;
  };
}
export class TiQueue extends EventEmitter {
  private pool: mysql.Pool;
  private pollingInterval: number;
  private concurrency: number;
  private isRunning: boolean = false;
  private isPaused: boolean = false;
  private activeJobs: number = 0;
  private cronJobs: Map<number, CronJob> = new Map();
  private workers: ChildProcess[] = [];
  private logger?: winston.Logger;
  private useSeparateProcess: boolean;
  private workerFile?: string;
  private pollIntervalId?: NodeJS.Timeout;
  private queueName: string;
  private jobHandlers: Map<string, (job: Job) => Promise<any>> = new Map();

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
      this.logger = winston.createLogger({
        level: options.logging.level || "info",
        format: winston.format.simple(),
        transports: [
          new winston.transports.Console(),
          new winston.transports.File({
            filename: options.logging.filename || "job_queue.log",
          }),
        ],
      });
    }

    this.initPromise = this.init();

    if (options.autoStart) {
      this.initPromise.then(() => this.start());
    }
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

  private initPromise: Promise<void>;

  async init(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS jobs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        queueName VARCHAR(50) NOT NULL,
        jobId CHAR(36) DEFAULT UUID(),
        payload JSON,
        status ENUM('queued', 'processing', 'completed', 'failed') DEFAULT 'queued',
        runAt TIMESTAMP NOT NULL,
        retryOnFail BOOLEAN DEFAULT FALSE,
        retryOnFailInterval VARCHAR(10) DEFAULT '1m',
        priority INT DEFAULT 0,
        attempts INT DEFAULT 0,
        maxAttempts INT DEFAULT 3,
        maxRetryOnFailAttempts INT DEFAULT 3,
        retryOnFailAttempts INT DEFAULT 0,
        cron VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);
    await this.pool.query(
      "CREATE INDEX IF NOT EXISTS idx_queueName_status_runAt_priority_job_id ON jobs (queueName, status, runAt, priority, jobId)"
    );
  }

  async add(payload: any, options: JobOptions = {}): Promise<number> {
    await this.initPromise;
    const {
      delay = "0s",
      priority = 0,
      maxAttempts = 3,
      cron,
      jobId: customJobId,
      retryOnFailInterval = "1m",
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
    if (this.isRunning) return;
    this.isRunning = true;
    this.isPaused = false;
    this.pollIntervalId = setInterval(
      () => this.processJobs(),
      this.pollingInterval
    );

    this.emit("start");
    this.log("info", "Job queue started");
  }

  stop(): void {
    if (!this.isRunning) return;
    this.isRunning = false;
    if (this.pollIntervalId) {
      clearInterval(this.pollIntervalId);
      this.pollIntervalId = undefined;
    }
    this.cronJobs.forEach((job) => job.stop());
    this.cronJobs.clear();
    this.workers.forEach((worker) => worker.kill());
    this.workers = [];
    this.emit("stop");
    this.log("info", "Job queue stopped");
  }

  pause(): void {
    this.isPaused = true;
    this.emit("pause");
    this.log("info", "Job queue paused");
  }

  resume(): void {
    this.isPaused = false;
    this.emit("resume");
    this.log("info", "Job queue resumed");
  }

  private async processJobs(): Promise<void> {
    if (this.isPaused || this.activeJobs >= this.concurrency) return;

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
        [this.queueName, this.concurrency - this.activeJobs]
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
        this.emit("start", job);
        this.log(
          "info",
          `Starting job ${job.jobId} in queue ${this.queueName}`
        );
        this.processJob(job);
      }
    } catch (error) {
      await connection.rollback();
      this.log("error", "Error processing jobs:", error);
      this.emit("error", error);
    } finally {
      connection.release();
    }
  }

  private processJob(job: Job): void {
    if (this.useSeparateProcess) {
      this.processJobInSeparateProcess(job);
    } else {
      this.processJobInline(job);
    }
  }

  private async processJobInline(job: Job): Promise<void> {
    try {
      const handler = this.jobHandlers.get(this.queueName);
      if (!handler) {
        throw new Error(`No handler registered for queue ${this.queueName}`);
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
    this.emit("completed", job, result);
  }

  private async handleFailedJob(job: Job, error: Error): Promise<void> {
    try {
      if (job.attempts >= job.maxAttempts) {
        if (
          job.retryOnFail &&
          job.retryOnFailAttempts < job.maxRetryOnFailAttempts
        ) {
          // Job has exhausted its initial attempts, but can still be retried
          const nextRetryAt = new Date(
            Date.now() + this.parseTime(job.retryOnFailInterval)
          );
          await this.pool.query(
            'UPDATE jobs SET status = "queued", attempts = 0, runAt = ?, retryOnFailAttempts = retryOnFailAttempts + 1 WHERE id = ?',
            [nextRetryAt, job.id]
          );
          this.log(
            "info",
            `Job ${job.jobId} failed and scheduled for retry at ${nextRetryAt}`
          );
          this.emit("retry", job, error);
        } else {
          // Job has exhausted all attempts and retries
          await this.pool.query(
            'UPDATE jobs SET status = "failed" WHERE id = ?',
            [job.id]
          );
          this.log(
            "warn",
            `Job ${job.jobId} failed after ${job.attempts} attempts and ${job.retryOnFailAttempts} retries`
          );
          this.emit("failed", job, error);
        }
      } else {
        // Job has not exhausted its initial attempts, reschedule it
        await this.pool.query(
          'UPDATE jobs SET status = "queued", runAt = NOW() + INTERVAL ? SECOND WHERE id = ?',
          [this.parseTime(job.retryOnFailInterval) / 1000, job.id]
        );
        this.log("info", `Job ${job.jobId} failed and rescheduled for retry`);
        this.emit("retry", job, error);
      }
    } catch (err) {
      this.log("error", `Error handling failed job ${job.jobId}:`, err);
      this.emit("error", err);
    }
  }
  async getJobStatus(jobId: number): Promise<Job | null> {
    const [_jobs] = await this.pool.query(
      "SELECT * FROM jobs WHERE id = ? AND queueName = ?",
      [jobId, this.queueName]
    );
    const jobs = _jobs as Job[];
    return jobs[0] || null;
  }

  async cancelJob(jobId: number): Promise<boolean> {
    const [result] = await this.pool.query<mysql.ResultSetHeader>(
      'UPDATE jobs SET status = "failed" WHERE id = ? AND queueName = ? AND status IN ("queued", "processing")',
      [jobId, this.queueName]
    );
    return result.affectedRows > 0;
  }

  async retryJob(jobId: number): Promise<boolean> {
    const [result] = await this.pool.query<mysql.ResultSetHeader>(
      'UPDATE jobs SET status = "queued", attempts = 0, runAt = NOW() WHERE id = ? AND queueName = ? AND status = "failed"',
      [jobId, this.queueName]
    );
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

  private log(level: string, message: string, meta?: any): void {
    if (this.logger) {
      this.logger.log(level, message, meta);
    }
  }

  private setupCronJob(jobId: number, cronExpression: string): void {
    const job = new CronJob(cronExpression, () => this.triggerCronJob(jobId));
    this.cronJobs.set(jobId, job);
    job.start();
  }

  private async triggerCronJob(jobId: number): Promise<void> {
    await this.pool.query(
      'UPDATE jobs SET runAt = NOW(), attempts = 0 WHERE id = ? AND queueName = ? AND status != "processing"',
      [jobId, this.queueName]
    );
  }

  async shutdown(): Promise<void> {
    this.stop();
    await this.pool.end();
  }
}

export default TiQueue;
